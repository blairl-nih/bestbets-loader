const { AbstractRecordSource }      = require('loader-pipeline');
const { AllHtmlEntities }           = require('html-entities')
const CDEPublishedContentListing    = require('cde-published-content-listing')
const https                         = require('https');
const { HttpsAgent }                = require('agentkeepalive');

const entities = new AllHtmlEntities();

/**
 * This class implements a source of BestBets Category records
 * using the PublishedContent "service" with the CDE.
 */
class CDEBestbetsSource extends AbstractRecordSource {

    /**
     * Creates a new instance of a CDEBestbetsSource
     * @param {logger} logger An instance of a logger.
     * @param {Object} client CDE Published Content Listing client
     */
    constructor(logger, client) {
        super(logger);

        // Set Listing Client
        this.client = client;
        
    }

    /**
     * Called before any resources are loaded.
     */
    async begin() {
        return;
    }

    /**
     * Get a collection of resources from this source
     */
    async getRecords() {

        let rtnCategories = [];

        this.logger.debug("CDEBestbetsSource:getRecords - Beginning Fetch");

        const bbListings = await this.getCategoryList();
        
        //download the content
        const results = await Promise.all(
            bbListings.Files.map(async (item) => {
                rtnCategories.push(await this.getCategory(item));
            })
        );

        this.logger.debug("CDEBestbetsSource:getResources - Completed Fetch");

        return rtnCategories;
    }

    /**
     * Internal function to get the list of categories from the published content listing
     */
    async getCategoryList() {
        let bbListings;

        //Get list of content from github
        try {
            bbListings = await this.client.getItemsForPath('BestBets');
        } catch (err) {
            this.logger.error(`Could not fetch Best Bets Categories List from server.`);
            this.logger.error(err)
            throw new Error(`Could not fetch Best Bets Categories List from server.`);
        }

        return bbListings
    }

    /**
     * Downloads a category.
     * @param {object} catListing The listing item
     * @returns {object} the resource
     */
    async getCategory(catListing) {

        let cat;

        try {
            cat = await this.client.getPublishedFile(catListing);
        } catch (err) {
            this.logger.error(`Could not fetch ${catListing.FullWebPath}`);
            throw new Error(`Could not fetch ${catListing.FullWebPath}`);
        }

        const root = 'cde:BestBetsCategory';

        if (!cat[root]) {
            throw new Error(`Invalid BestBets Category, ${catListing.FullWebPath}`);
        }

        const cleanCat = {
            categoryID: this.cleanToString(cat[root].CategoryId),
            categoryName: this.cleanToString(cat[root].CategoryName),
            categoryWeight: this.cleanToInt(cat[root].CategoryWeight),
            isExactMatch: this.cleanToBool(cat[root].IsExactMatch),
            language: this.getLang(this.cleanToString(cat[root].Language)),
            display: this.cleanToBool(cat[root].Display),
            includeSynonyms: this.extractSynonyms(cat[root].IncludeSynonyms),
            excludeSynonyms: this.extractSynonyms(cat[root].ExcludeSynonyms),
        }

        if (cleanCat.language == "") {
            throw new Error(`Invalid BestBets Category, ${catListing.FullWebPath}, language is empty or unknown`);
        }

        return cleanCat;
    }

    /**
     * Validates the lang and gets a clean ISO 639-1 code if
     * it is a valid language/local.
     * (You will encounter multiple versions in the XML)
     * @param {*} cleanedLang 
     */
    getLang(cleanedLang) {
        if (!cleanedLang || cleanedLang === '') {
            return "";
        }

        switch(cleanedLang.toLowerCase()) {
            case 'en': 
            case 'eng':
            case 'en-us':
                return 'en';

            case 'es':
            case 'esp':
            case 'es-us':
                return 'es';
        }

        //Unknown
        return "";
    }

    /**
     * Extracts a list of Synonyms from a Synonym node
     * @param {*} synElement THe element containing the synonyms
     */
    extractSynonyms(synElement) {        
        if (synElement && 
            synElement.length && 
            synElement[0].synonym && 
            synElement[0].synonym.length 
        ) {
            return synElement[0].synonym.map(syn => ({
                name: this.cleanToString(syn["_"]),
                isExactMatch: this.cleanToBool(syn['$']["IsExactMatch"])
            }))
        } else {
            return [];
        }
    }

    /**
     * This cleans up and converts a string to a boolean
     * @param {*} val 
     */
    cleanToBool(val) {
        //The XML files always use 'true' for true. 
        //(well, when they don't have newlines preceeding them...)
        return this.cleanToString(val) === 'true';
    }

    /**
     * This cleans up and converts a string to a boolean
     * @param {*} val 
     */
    cleanToInt(val) {
        return parseInt(this.cleanToString(val));
    }

    /**
     * This cleans up and converts a string to a boolean
     * @param {*} val 
     */
    cleanToString(val) {
        if (!val) {
            return "";
        }
        
        return Array.isArray(val) ? 
            (
                val.length ? 
                entities.decode(val[0].trim()) : 
                ""
            ) : 
            entities.decode(val.trim());
    }

    /**
     * Method called after all resources have been loaded
     */
    async end() {
        return;
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
     * @param {string} config.hostname The hostname for the source
     */
    static ValidateConfig(config) {
        let errors = [];

        if (!config.hostname) {
            errors.push(new Error("You must supply a source hostname"));
        }        

        if (
            config.socketLimit && 
            (typeof config.socketLimit !== 'number' || config.socketLimit <=0) 
        ) {
            errors.push(new Error("socketLimit must be a number greater than 0"));
        }

        return errors;
    }    

    /**
     * A static helper function to get a configured source instance
     * @param {Object} logger the logger to use
     * @param {Object} config configuration parameters to use for this instance. See GithubResourceSource constructor.
     * @param {string} config.hostname The hostname for the source
     */
    static async GetInstance(logger, config) {

        if (!config) {
            throw new Error("Config must be supplied");
        }

        if (!config.hostname) {
            throw new Error("You must supply a source hostname");
        }

        if (
            config.socketLimit && 
            (typeof config.socketLimit !== 'number' || config.socketLimit <=0) 
        ) {
            throw new Error("socketLimit must be a number greater than 0");
        }

        //TODO: Find a better way to manage the agent so there can be one agent per 
        //application.  (and thus one pool of sockets)
        const agent = new HttpsAgent({
            maxSockets: config.socketLimit ? config.socketLimit : 80
        });

        //Get instance of content listing with our custom https agent
        const client = new CDEPublishedContentListing(config.hostname, agent);

        return new CDEBestbetsSource(logger, client);
    }
}

module.exports = CDEBestbetsSource;