const { AbstractRecordLoader }  = require('loader-pipeline');
const elasticsearch             = require('elasticsearch');
const path                      = require('path');
const ElasticTools              = require('elastic-tools');

/**
 * This class implements an Elasticsearch Resource loader
 */
class ElasticBestbetsLoader extends AbstractRecordLoader {

    /**
     * Creates a new instance of an AbstractResourceLoader
     * @param {logger} logger An instance of a logger.
     * @param {ElasticTools} estools An instance of ElasticTools
     * @param {object} mappings the ES mapping config
     * @param {object} settings the ES mapping config
     * @param {object} config the step config
     * @param {object} config.daysToKeep the number of days of indices to keep
     * @param {object} config.minIndexesToKeep the number of days of indices to keep
     * @param {object} config.aliasName the alias name for this pipeline collection
     */
    constructor(
        logger,
        estools,
        mappings,
        settings,
        {
            daysToKeep = 10,
            minIndexesToKeep = 2,
            aliasName = false
        } = {}
    ) {
        super(logger);

        if (!aliasName || typeof aliasName !== 'string') {
            throw new Error("aliasName is required for the elastic loader");
        }
        this.aliasName = aliasName;

        if (!daysToKeep || typeof daysToKeep !== 'number') {
            throw new Error("daysToKeep is required for the elastic loader");
        }
        this.daysToKeep = daysToKeep;

        if (!minIndexesToKeep || typeof minIndexesToKeep !== 'number') {
            throw new Error("minIndexesToKeep is required for the elastic loader");
        }
        this.minIndexesToKeep = minIndexesToKeep;

        this.estools = estools;

        this.indexName = false;

        this.mappings = mappings;
        this.settings = settings;
    }

    /**
     * Called before any resources are loaded.
     */
    async begin() {
        this.logger.debug("ElasticBestbetsLoader:begin - Begin Begin");

        try {
            this.indexName = await this.estools.createTimestampedIndex(this.aliasName, this.mappings, this.settings);
        } catch (err) {
            this.logger.error(`Failed to create index ${this.indexName}`)
            throw err;
        }
        this.logger.debug("ElasticResourceLoader:begin - End Begin");
    }

    /**
     * @typedef Object BestbetMatch
     * @param {string} category The name of the Bestbet Category
     * @param {string} contentID The content ID of the Bestbet Category
     * @param {string} synonym The synonym text to index
     * @param {string} language The language of the category
     * @param {bool} isNegated Should this synonym be used for excluding display
     * @param {bool} isExact Must the text match exactly
     * @param {number} tokenCount The number of words the analyzed synonym would have
     */

    /**
     * Loads a resource into the data store
     * @param {BestbetMatch[]} matches an array of best bet matches
     */
    async loadRecord(matches) {

        if (matches.length === 0){
          throw new Error("A category resulted in 0 matches")
        }

        // The category display will be attached to only the match
        // for the main category, not the synonym matches.
        // So we need to find it
        const categoryDisplay = matches
          .filter(match => match.isCategory)
          .map(cat => ({
            contentid: cat.contentID,
            name: cat.category,
            weight: cat.weight,
            content: cat.categoryDisplay
          }))
          .shift();

        if (!categoryDisplay) {
          throw new Error(`Category ${matches[0].contentID} is missing its display`);
        }

        //Convert the array of matches into the expected bulk index list.
        const docArr = matches.reduce((ac, curr, ci) => [
          ...ac,
          [ `${curr.contentID}_${ci}`, {
              category: curr.category,
              contentid: curr.contentID,
              synonym: curr.synonym,
              language: curr.language,
              "is_negated": curr.isNegated,
              "is_exact": curr.isExact,
              tokencount: curr.tokenCount
          }]
        ], []);

        let res;
        try {
          res = await Promise.all([
            this.estools.indexDocumentBulk(this.indexName,"synonyms", docArr),
            this.estools.indexDocument(this.indexName, 'categorydisplay', categoryDisplay.contentid, categoryDisplay)
          ]);
        } catch (err) {
            this.logger.error(`Could not index synonyms for ${matches[0].contentID}`);
            throw err;
        }

        //Check to see if this is how we expected things to go...
        if (res[0].updated.length) {
            const message = `Category ${matches[0].contentID} appears to have duplicates`
            this.logger.error(message);
            throw new Error(message);
        } else if (res[0].errors.length) {
            const message = `Category ${matches[0].contentID} appears had document errors`
            this.logger.error(message);
            throw new Error(message);
        }
    }

    /**
     * Called upon a fatal loading error. Use this to clean up any items created on startup
     */
    async abort() {
        //remove the current index if we are aborting

        throw new Error("Not Implemented");
    }

    /**
     * Method called after all resources have been loaded
     */
    async end() {

        try {
            //optimize the index
            await this.estools.optimizeIndex(this.indexName);

            //swap the alias
            await this.estools.setAliasToSingleIndex(this.aliasName, this.indexName);

            //Clean up old indices
            try {
                await this.estools.cleanupOldIndices(this.aliasName, this.daysToKeep, this.minIndexesToKeep);
            } catch (err) {
                this.logger.error("Could not cleanup old indices");
                throw err;
            }

        } catch (err) {
            this.logger.error("Errors occurred during end process");
            throw err;
        }

    }

    /**
     * A static method to validate a configuration object against this module type's schema
     * @param {Object} config configuration parameters to use for this instance.
     * @param {string|string[]} config.eshosts An array of elasticsearch hosts
     * @param {number} config.daysToKeep the number of days to keep indices for.
     * @param {number} config.minIndexesToKeep the minimum number of indices to keep.
     * @param {number} config.aliasName the name of the alias to use for this collection.
     * @param {string} config.mappingPath the path to the mappings file
     * @param {string} config.settingsPath the path to the settings file
     */
    static ValidateConfig(config) {
        let errors = [];

        if (!config["mappingPath"] || typeof config.mappingPath !== 'string') {
            errors.push( new Error("mappingPath is required for the elastic loader") );
        }

        if (!config["settingsPath"] || typeof config.settingsPath !== 'string') {
            errors.push( new Error("settingsPath is required for the elastic loader") );
        }

        //TODO: This should be a better check...
        if (!config.eshosts) {
            errors.push( new Error("eshosts is required for the elastic loader"));
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
     * @param {string|string[]} config.eshosts An array of elasticsearch hosts
     * @param {number} config.daysToKeep the number of days to keep indices for.
     * @param {number} config.minIndexesToKeep the minimum number of indices to keep.
     * @param {number} config.aliasName the name of the alias to use for this collection.
     * @param {string} config.mappingPath the path to the mappings file
     * @param {string} config.settingsPath the path to the settings file
     */
    static async GetInstance(logger, config) {

        const appRoot = path.join(__dirname, '..', '..');

        let mappings;
        if (!config["mappingPath"] || typeof config.mappingPath !== 'string') {
            throw new Error("mappingPath is required for the elastic loader");
        }
        const mapFullPath = path.join(appRoot, config["mappingPath"]);
        try {
            mappings = require(mapFullPath);
        } catch (err) {
            throw new Error(`mappingPath cannot be loaded: ${mapFullPath}`);
        }

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

        //TODO: This should be a better check...
        if (!config.eshosts) {
            throw new Error("eshosts is required for the elastic loader");
        }

        if (
            config.socketLimit &&
            (typeof config.socketLimit !== 'number' || config.socketLimit <= 0)
        ) {
            throw new Error("socketLimit must be a number greater than 0");
        }

        const estools = new ElasticTools(logger, new elasticsearch.Client({
            hosts: config.eshosts,
            maxSockets: config.socketLimit ?
                        config.socketLimit :
                        80,
            keepAlive: true
        }))

        return new ElasticBestbetsLoader(
            logger,
            estools,
            mappings,
            settings,
            {
                ...config,
                settingsPath:undefined,
                mappingPath:undefined,
                eshosts: undefined,
                socketLimit: undefined
            }
        );
    }

}

module.exports = ElasticBestbetsLoader;
