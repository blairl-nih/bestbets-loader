const {
  AbstractRecordSource
} = require('loader-pipeline');
const { AllHtmlEntities }           = require('html-entities')
const octokit                       = require('@octokit/rest');
const axios                         = require('axios');
const https                         = require('https');
const util                          = require('util');
const github_url_parse              = require('parse-github-repo-url');
const { parseString }               = require('xml2js');

const parseStringAsync  = util.promisify(parseString);

const entities = new AllHtmlEntities();

/**
 * This class implements a Bestbets Source wherin the content lives in the
 * bestbets-content structure of Github.
 */
class GithubBestbetsSource extends AbstractRecordSource {

  /**
   * Creates a new instance of a GithubResourceSource
   * @param {logger} logger An instance of a logger.
   * @param {octokit} client An initialized GitHub client from octokit/rest.
   * @param {Object} axclient Axios client for making HTTP(s) requests
   * @param {Object} param2 A configuration object
   * @param {string} param2.repoUrl The URL for the source github repo
   * @param {string} param2.contentPath The path within the repo to the content. (DEFAULT: /content)
   * @param {string} param2.branchName The name of the branch to use. (DEFAULT: master)
   */
  constructor(logger, client, axclient, {
    repoUrl = false,
    contentPath = '/content',
    branchName = false
  } = {}) {
    super(logger);

    if (repoUrl === false) {
      throw new Error("You must supply a repository URL");
    }

    // Set Github Client
    this.client = client;

    // Set HTTP(S) Agent
    this.axclient = axclient;

    //break up the repo url
    try {
      const [owner, repo] = github_url_parse(repoUrl);
      this.owner = owner;
      this.repo = repo;
    } catch (err) {
      this.logger.error(`Could not parse repoUrl, ${repoUrl}`);
      throw new Error("Failed to parse github url");
    }

    this.contentPath = contentPath;

    this.branchName = branchName;

  }

  /**
   * Called before any resources are loaded.
   */
  async begin() {
    return;
  }

  /**
   * Get a collection of records from this source
   */
  async getRecords() {

    this.logger.debug("GithubResourceSource:getRecords - Beginning Fetch");

    let contentList;
    //Get list of content from github
    try {
      contentList = await this.getContentList();
    } catch (err) {
      this.logger.error(`Could not fetch resources from GitHub https://github.com/${this.owner}/${this.repo}${this.contentPath}`);
      throw err;
    }

    let categories;
    //download to the content and map it
    try {
      categories = await Promise.all(
        contentList.map(async(contentUrl) => {
          const content = await this.getContent(contentUrl);
          return await this.mapContentToCat(content)
        })
      )
    } catch (err) {
      this.logger.error(`Could not fetch individual resources from GitHub https://github.com/${this.owner}/${this.repo}${this.contentPath}`);
      throw err;
    }

    this.logger.debug("GithubResourceSource:getResources - Completed Fetch");

    return categories;
  }

  /**
   * Internal function to get the list of content in the content folder
   * @return {array} an array of the URLs to download
   */
  async getContentList() {

    let options = {
      owner: this.owner,
      repo: this.repo,
      path: this.contentPath.charAt(0) == '/' ?
        this.contentPath.slice(1) : this.contentPath,
    }

    if (this.branchName) {
      options = {
        ...options,
        branchName: this.branchName
      }
    }

    var result;
    try {
      result = await this.client.repos.getContents(options);
    } catch (err) {
      throw new Error('Could not fetch Best Bets Categories List from server.');
    }

    const regEx = /.*\.xml$/;

    const downloadList = result.data
      .filter(r => r.type === 'file' && regEx.test(r.download_url))
      .map(r => r.download_url);

    return downloadList;
  }

  /**
   * Downloads a best bet from github and converts it to a category
   * @param {*} contentUrl The raw url for the content
   * @returns {Object} the resource
   */
  async getContent(contentUrl) {

    let response;

    try {
      response = await this.axclient.get(contentUrl, {
        responseType: 'text'
      });
    } catch (err) {
      this.logger.error(`Could not fetch ${contentUrl}`);
      throw new Error(`Could not fetch ${contentUrl}`);
    }

    return response.data;
  }

  /**
   * Gets a category object from an XML content file.
   *
   * @param {*} content
   */
  async mapContentToCat(content) {
    let cat;
    try {
      cat = await parseStringAsync(content);
    } catch(err) {
      throw new Error('Cannot process XML')
    }

    const root = 'cde:BestBetsCategory';

    if (!cat[root]) {
        throw new Error(`Invalid BestBets Category, ${contentUrl}`);
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
        categoryDisplay: this.getDisplay(cat[root].CategoryDisplay),
    }

    if (cleanCat.language === "") {
        throw new Error(`Invalid BestBets Category, ${catListing.FullWebPath}, language is empty or unknown`);
    }

    return cleanCat;
  }

  /**
   * Gets out the category display, or an empty string.
   *
   * @param {*} content the content to extract
   */
  getDisplay(content) {
    if (!content) {
      return "";
    }

    return Array.isArray(content) ?
      (
        content.length ?
        content[0].trim() :
        ""
      ) :
      content.trim();
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

    switch (cleanedLang.toLowerCase()) {
      case 'en':
      case 'eng':
      case 'en-us':
        return 'en';

      case 'es':
      case 'esp':
      case 'es-us':
        return 'es';
      default:
        return "";
    }
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
   * @param {string} config.repoUrl The URL for the source github repo
   * @param {string} config.contentPath The path within the repo to the resources. (DEFAULT: /resources)
   * @param {string} param2.branchName The name of the branch to use. (DEFAULT: master)
   */
  static ValidateConfig(config) {
    let errors = [];

    if (!config.repoUrl) {
      errors.push(new Error("You must supply a repository URL"));
    }

    return errors;
  }

  /**
   * A static helper function to get a configured source instance
   * @param {Object} logger the logger to use
   * @param {Object} config configuration parameters to use for this instance. See GithubResourceSource constructor.
   */
  static async GetInstance(logger, config) {

    if (!config) {
      throw new Error("Config must be supplied");
    }

    //TODO: Find a better way to manage the agent so there can be one agent per
    //application.  (and thus one pool of sockets)
    const agent = new https.Agent({
      keepAlive: true,
      maxSockets: 80
    });

    //Get instance of axios with our custom https agent
    const axiosInstance = axios.create({
      httpsAgent: agent
    })

    // We will probably need to authenticate to get around the rate limits
    // they are based on IP address, which for us *could* be a major limiter.
    const client = octokit({
      // Since we will be scraping the GitHub site we will be making a lot of calls
      // the following options will make sure that we do not kill the computer's
      // sockets that it runs on.
      request: {
        agent
      }
    });

    //should authenticate here
    if (config["authentication"]) {
      //Per the docs this is synchronous
      client.authenticate(config["authentication"]);
    }

    return new GithubBestbetsSource(logger, client, axiosInstance, config);
  }
}

module.exports = GithubBestbetsSource;
