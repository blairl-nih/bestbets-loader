const { AbstractRecordTransformer } = require('loader-pipeline');
const elasticsearch                 = require('elasticsearch');
const path                          = require('path');

const timeout = ms => new Promise(res => setTimeout(res, ms))

/**
 * This class implements a Record transformer that transforms a category
 * into an array of match records
 */
class CategoryToMatchTransformer extends AbstractRecordTransformer {

    /**
     * Create a new instance of a CategoryToMatchTransformer
     * @param {object} logger The logger to use
     * @param {object} esclient An instance of an elasticsearch client
     * @param {object} config The config for this step
     * @param {object} config.tokenizer The tokenizer setting for the analyzer
     * @param {object} config.filter The filter setting for the analyzer
     */
    constructor(logger, esclient, { tokenizer = false, filter = false} = {}) {
        super(logger);

        if (!esclient) {
            throw new Error("You must supply an Elasticsearch client")
        }
        this.esclient = esclient;

        let analyzerSetting = {};

        if (tokenizer) {
            analyzerSetting['tokenizer'] = tokenizer;
        }

        if (filter) {
            analyzerSetting['filter'] = filter;
        }

        this.analyzerSetting = Object.freeze(analyzerSetting);

        this.tokenCache = {};
        this.fetchQueue = {};
    }

    /**
     * Transforms the resource
     * @param {Object} data the object to be transformed
     * @returns the transformed object
     */
    async transform(data) {
        //Let's go fetch the token counts from the analyzer as a
        //batch.

        //First we need to extract all the unique names
        const namesToTokenize = this.extractNamesToTokenize(data);

        //Get the lookup dictionary of names -> counts
        const lookup = await this.tokenizeNames(namesToTokenize);

        //Create sets
        const incMatches = data.includeSynonyms
                            .map(syn => ({
                                ...(this.extractMatchFromSyn(data, false, syn)),
                                tokenCount: lookup[syn.name]
                            }));

        const exMatches = data.excludeSynonyms
                            .map(syn => ({
                                ...(this.extractMatchFromSyn(data, true, syn)),
                                tokenCount: lookup[syn.name]
                            }))
        // The category match will always be indexed. So we will attach the category
        // display to this object for the loader to rip it out.
        const catMatch = {
            category: data.categoryName,
            contentID: data.categoryID,
            synonym: data.categoryName,
            language: data.language,
            isNegated: false,
            isExact: data.isExactMatch,
            tokenCount: lookup[data.categoryName],
            categoryDisplay: data.categoryDisplay
        }

        //Make the new matches
        return [
            catMatch,
            ...incMatches,
            ...exMatches
        ];
    }

    /**
     *
     * @param {*} cat
     * @param {*} isNeg Is this match negated
     * @param {*} synArr
     */
    extractMatchFromSyn(cat, isNeg, syn) {
        return {
            category: cat.categoryName,
            contentID: cat.categoryID,
            synonym: syn.name,
            language: cat.language,
            isNegated: isNeg,
            isExact: syn.isExactMatch,
            tokenCount: 0
        }
    }


    /**
     * Extracts all the match names that a category would yield
     * @param {*} cat the best bets category
     */
    extractNamesToTokenize(cat) {
        return [
            cat.categoryName,
            ...cat.includeSynonyms.map(syn => syn.name),
            ...cat.excludeSynonyms.map(syn => syn.name)
        ].reduce((ac, curr) => {
            if (!ac.some((el => el.toLowerCase() === curr.toLowerCase()))) {
                return [
                    ...ac,
                    curr
                ]
            } else {
                return ac;
            }
        }, []);
    }

    /**
     * Gets token counts for an array of names
     * @param {*} nameArr an array of names to tokenize
     * @returns {object} a dictionary of names to their token counts
     */
    async tokenizeNames(nameArr) {
        const results = await Promise.all(nameArr.map(async (name) => {
                const count = await this.tokenizeNameWrap(name);
                return {
                    name,
                    count
                }
            }
        ));

        const lookup = results.reduce(
                        (ac,curr) => {
                            return {
                                ...ac,
                                [curr.name]: curr.count
                            }
                        }, {});
        return lookup;
    }

    /**
     * This is a wraper around tokenizeName to allow for:
     * 1. Looking up a previous fetched item from the cache
     * 2. Avoid fetching the same term at the same time
     * NOTE: while the list of names to tokenize is unique,
     * multiple categories could be processing.
     * @param {*} name
     */
    async tokenizeNameWrap(name) {
        if (this.tokenCache[name.toLowerCase()]) {
            return this.tokenCache[name.toLowerCase()];
        } else {
            if (this.fetchQueue[name.toLowerCase()]) {
                // Set timeout
                await timeout(100);
                //and wake up after (hopefully) this name has finished
                //fetching. Then call ourselves again, hoping that fetch
                //has finished.

                return await this.tokenizeNameWrap(name);
            } else {
                this.fetchQueue[name.toLowerCase()] = true;
                this.tokenCache[name.toLowerCase()] = await this.tokenizeName(name);
                this.fetchQueue[name.toLowerCase()] = false;
                return this.tokenCache[name.toLowerCase()];
            }
        }
    }

    /**
     * Gets the token count of a given string using the analyzer.
     * @param {*} name the name to get the token count.
     * @returns {number} the number of tokens after analysis
     */
    async tokenizeName(name) {
        let res;

        try {
            res = await this.esclient.indices.analyze({
                body: {

                ...this.analyzerSetting,
                text: name
                }
            });
        } catch (err) {
            this.logger.error(`Could not analyze ${name}. ${err.message}`);
            throw err;
        }

        return res.tokens.length
    }

    /**
     * Called before any resources are transformed -- load mappers and anything else here.
     */
    async begin() {
        return;
    }

    /**
     * Method called after all resources have been transformed
     */
    async end() {
        return; //I have nothing to do here...
    }

    /**
     * Called upon a fatal loading error. Use this to clean up any items created on startup
     */
    async abort() {
        return;
    }

    /**
     * A static method to validate a configuration object against this module type's schema
     * @param {Object} config configuration parameters to use for this instance.
     * @param {string} config.eshosts The Elasticsearch hosts
     * @param {string} config.settingsPath The path to the BestBets index settings object
     * @param {string} config.analyzer The analyzer to use for tokenization from the settings
     */
    static ValidateConfig(config) {
        let errors = [];

        //TODO: This should be a better check...
        if (!config["eshosts"]) {
            errors.push( new Error("eshosts is required for the transformer"));
        }

        if (!config["settingsPath"] || typeof config.settingsPath !== 'string') {
            errors.push( new Error("settingsPath is required for the transformer") );
        }

        if (!config["analyzer"] || typeof config.analyzer !== 'string') {
            errors.push( new Error("You must supply the name of analyzer defined in the settings"));
        }

        if (
            config.socketLimit &&
            (typeof config.socketLimit !== 'number' || config.socketLimit <= 0)
        ) {
            errors.push(new Error("socketLimit must be a number greater than 0"));
        }

        return errors;
    }

    /**
     * A static helper function to get a configured source instance
     * @param {Object} logger the logger to use
     * @param {Object} config configuration parameters to use for this instance.
     * @param {string} config.settingsPath The path to the BestBets index settings object
     * @param {string} config.analyzer The analyzer to use for tokenization from the settings
     */
    static async GetInstance(logger, config) {

        const appRoot = path.join(__dirname, '..', '..');

        //TODO: This should be a better check...
        if (!config["eshosts"]) {
            throw new Error("eshosts is required for the elastic loader");
        }

        if (
            config.socketLimit &&
            (typeof config.socketLimit !== 'number' || config.socketLimit <= 0)
        ) {
            throw new Error("socketLimit must be a number greater than 0");
        }

        const client = new elasticsearch.Client({
            hosts: config.eshosts,
            apiVersion: '5.6',
            maxSockets: config.socketLimit ?
                        config.socketLimit :
                        80,
            keepAlive: true
        });

        let settings;
        if (!config["settingsPath"] || typeof config.settingsPath !== 'string') {
            throw new Error("settingsPath is required for the elastic loader");
        }
        const settingsFullPath = path.join(appRoot, config["settingsPath"]);
        try {
            settings = require(settingsFullPath);
        } catch (err) {
            throw new Error(`settingsPath cannot be loaded: ${settingsFullPath}`);
        }

        if (!config["analyzer"] || typeof config.analyzer !== 'string') {
            throw new Error("You must supply the name of analyzer defined in the settings")
        }

        const analyzer = config.analyzer;

        if (!settings["settings"] ||
            !settings["settings"]["analysis"] ||
            !settings["settings"]["analysis"]["analyzer"] ||
            !settings["settings"]["analysis"]["analyzer"][analyzer]
        ) {
            throw new Error("Could not find analyzer in settings")
        }

        const tokenizer = settings.settings.analysis.analyzer[analyzer]["tokenizer"];
        const filter = settings.settings.analysis.analyzer[analyzer]["filter"];

        return new CategoryToMatchTransformer(logger, client, {
            tokenizer,
            filter
        });
    }

}

module.exports = CategoryToMatchTransformer;
