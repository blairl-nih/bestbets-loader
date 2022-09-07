const { Octokit }           = require('@octokit/rest');
const axios                 = require('axios');
const https                 = require('https');
const fs                    = require('fs');
const util                  = require('util');
const moment                = require('moment');
const nock                  = require('nock');
const path                  = require('path');
const winston               = require('winston');
const { parseString }       = require('xml2js');
const WinstonNullTransport  = require('winston-null-transport');

const GithubBestbetsSource  = require('../github-bestbets-source');

// Get async readFile.
const readFileAsync = util.promisify(fs.readFile);

// Async version of parse string
const parseStringAsync  = util.promisify(parseString);


const logger = winston.createLogger({
  level: 'debug',
  format: winston.format.simple(),
  transports: [
    new WinstonNullTransport()
  ]
});

beforeAll(() => {
  nock.disableNetConnect();
});

//After each test, cleanup any remaining mocks
afterEach(() => {
  nock.cleanAll();
});

afterAll(() => {
  nock.enableNetConnect();
});

const VALID_CONFIG = {
  repoUrl: 'https://github.com/NCIOCPL/bestbets-content'
};

/*************
 * Expected Best Bets
 **************/
const EXPECTED_BB = Object.freeze({
  "431121": {
    categoryID: "431121",
    categoryName: "Fotos de cáncer",
    categoryWeight: 30,
    isExactMatch: true,
    language: 'es',
    display: true,
    includeSynonyms: [
      { name: "fotos", isExactMatch: false },
      { name: "imagenes", isExactMatch: false },
      { name: "fotos de cancer", isExactMatch: true },
      {
        name: "imagenes de cancer",
        isExactMatch: true
      },
      {
        name: "imágenes de cáncer",
        isExactMatch: true
      },
      {
        name: "imagenes de cáncer",
        isExactMatch: true
      },
      {
        name: "imágenes de cancer",
        isExactMatch: true
      },
      {
        name: "imágenes",
        isExactMatch: false
      },
      {
        name: "imajenes",
        isExactMatch: false
      },
      {
        name: "fotografias",
        isExactMatch: false
      },
    ],
    excludeSynonyms: [{
      name: "piel",
      isExactMatch: false
    }, ],
    categoryDisplay: "<div class=\"managed list\"><ul><li class=\"general-list-item general list-item\"><!-- cgvSnListItemGeneral --><!-- Image --><!-- End Image --><div class=\"title-and-desc title desc container\"><a class=\"title\" href=\"http://visualsonline.cancer.gov/\">Visuals Online</a><!-- start description --><div class=\"description\"><p class=\"body\">Base de datos del NCI con fotografías de médicos y científicos dedicados a la investigación del cáncer e imágenes de tratamientos de pacientes con cáncer. También se encuentran imágenes biomédicas y de ciencias, y fotos de los directores y el personal del NCI.</p></div><!-- end description --></div><!-- end title & desc container --></li></ul></div>"
  },
  "1109313": {
    categoryID: "1109313",
    categoryName: "Mantle Cell Lymphoma",
    categoryWeight: 300,
    isExactMatch: false,
    language: 'en',
    display: true,
    includeSynonyms: [],
    excludeSynonyms: [],
    categoryDisplay: "<div class=\"managed list\"><ul><li class=\"general-list-item general list-item\"><!-- cgvSnListItemGeneral --><!-- Image --><!-- End Image --><div class=\"title-and-desc title desc container\"><a class=\"title\" href=\"/types/lymphoma/patient/adult-nhl-treatment-pdq\">Adult Non-Hodgkin Lymphoma Treatment (PDQ®)–Patient Version</a><!-- start description --><div class=\"description\"><p class=\"body\">Non-Hodgkin lymphoma (NHL) options include chemotherapy, radiation, targeted therapy, plasmapheresis, surveillance, stem cell transplant, and surgery. Learn more about types of NHL and treatments in this expert-reviewed summary.</p></div><!-- end description --></div><!-- end title & desc container --></li></ul></div>"
  },
  "1045389": {
    categoryID: "1045389",
    categoryName: "Cancer Research Ideas",
    categoryWeight: 100,
    isExactMatch: false,
    language: 'en',
    display: true,
    includeSynonyms: [{
      name: "Clinical Trial Ideas",
      isExactMatch: false
    }],
    excludeSynonyms: [],
    categoryDisplay: "<div class=\"managed list\"><ul><li class=\"general-list-item general list-item\"><!-- cgvSnListItemGeneral --><!-- Image --><!-- End Image --><div class=\"title-and-desc title desc container\"><a class=\"title\" href=\"https://cancerresearchideas.cancer.gov\">Cancer Research Ideas</a><!-- start description --><div class=\"description\"><p class=\"body\">An online platform for cancer researchers to submit their best scientific ideas for bringing about a decade’s worth of advances in 5 years, making more therapies available to more patients, and spurring progress in cancer prevention, treatment, and care. </p></div><!-- end description --></div><!-- end title & desc container --></li></ul></div>"
  },
  "35884": {
    categoryID: "35884",
    categoryName: "Tobacco Control",
    categoryWeight: 110,
    isExactMatch: false,
    language: 'en',
    display: true,
    includeSynonyms: [],
    excludeSynonyms: [{
        name: "monograph",
        isExactMatch: false
      },
      {
        name: "Branch",
        isExactMatch: false
      }
    ],
    categoryDisplay: "<div class=\"managed list\"><ul><li class=\"general-list-item general list-item\"><!-- cgvSnListItemGeneral --><!-- Image --><!-- End Image --><div class=\"title-and-desc title desc container\"><a class=\"title\" href=\"https://cancercontrol.cancer.gov/brp/tcrb/monographs/\">Tobacco Control Monograph Series</a><!-- start description --><div class=\"description\"><p class=\"body\">NCI established the Tobacco Control Monograph series in 1991 to provide ongoing and timely information about emerging public health issues in smoking and tobacco use control.</p></div><!-- end description --></div><!-- end title & desc container --></li><li class=\"general-list-item general list-item\"><!-- cgvSnListItemGeneral --><!-- Image --><!-- End Image --><div class=\"title-and-desc title desc container\"><a class=\"title\" href=\"http://cancercontrol.cancer.gov/brp/tcrb/\">Tobacco Control Research Branch</a><!-- start description --><div class=\"description\"><p class=\"body\">TCRB, within NCI's Division of Cancer Control and Population Sciences, leads and collaborates on research and disseminates evidence-based findings to prevent, treat, and control tobacco use.</p></div><!-- end description --></div><!-- end title & desc container --></li></ul></div>"
  }
})

/***********************************
 *  Create real instances of services
 ***********************************/
const real_agent = new https.Agent({ keepAlive: true, maxSockets: 80 });
const real_axiosInstance = axios.create({ httpsAgent: real_agent })
const real_octokit_client = new Octokit({ request: {agent: real_agent} });

// Setup the source to test
const source = new GithubBestbetsSource(
  logger,
  real_octokit_client,
  real_axiosInstance,
  {
    ...VALID_CONFIG
  }
)

describe('GithubBestbetsSource', () => {

  describe('constructor', () => {

    const agent = new https.Agent({
      keepAlive: true,
      maxSockets: 80
    });
    const client = new Octokit({
      request: {agent}
    });

    const axclient = axios.create({
      httpsAgent: agent
    });


    it('Creates with defaults', () => {
      const testsource = new GithubBestbetsSource(logger, client, axclient, VALID_CONFIG);
      expect(testsource.client).toBe(client);
      expect(testsource.axclient).toBe(axclient);
      expect(testsource.owner).toEqual("NCIOCPL");
      expect(testsource.repo).toEqual("bestbets-content");
      expect(testsource.contentPath).toEqual("/content");
      expect(testsource.branchName).toBeFalsy();
    });

    it('Creates with defaults, custom ', () => {
      const testsource = new GithubBestbetsSource(
        logger,
        client,
        axclient, {
          ...VALID_CONFIG,
          contentPath: '/test'
        });
      expect(testsource.client).toBe(client);
      expect(testsource.axclient).toBe(axclient);
      expect(testsource.owner).toEqual("NCIOCPL");
      expect(testsource.repo).toEqual("bestbets-content");
      expect(testsource.contentPath).toEqual("/test");
      expect(testsource.branchName).toBeFalsy();
    });

    it('Creates with defaults, branch ', () => {
      const testsource = new GithubBestbetsSource(
        logger,
        client,
        axclient, {
          ...VALID_CONFIG,
          contentPath: '/test',
          branchName: 'test'
        });
      expect(testsource.client).toBe(client);
      expect(testsource.axclient).toBe(axclient);
      expect(testsource.owner).toEqual("NCIOCPL");
      expect(testsource.repo).toEqual("bestbets-content");
      expect(testsource.contentPath).toEqual("/test");
      expect(testsource.branchName).toEqual('test');
    });

    it('Gracefully throws an error on bad repo url ', () => {
      expect(() => {
        const testsource = new GithubBestbetsSource(
          logger,
          client,
          axclient, {
            repoUrl: "chicken"
          });
      }).toThrowError("Failed to parse github url");
    });

    it('Gracefully throws an error missing repo url ', () => {
      expect(() => {
        const testsource = new GithubBestbetsSource(
          logger,
          client,
          axclient, {});
      }).toThrowError("You must supply a repository URL");
    });
  })

  /**
   * Tests for fetching the raw category
   */
  describe('getContent', () => {

    //Test fetches/parsing xml
    it.each([
        [
            'complex category',
            '431121'
        ],
        [
            'simple category',
            '1109313'
        ],
        [
            'cat include, no exclude',
            '1045389'
        ],
        [
            'cat exclude, no include',
            '35884'
        ]
    ])(
        'gets %s',
        async (name, catID) => {
          const host = 'https://raw.githubusercontent.com';
          const catPath = `/NCIOCPL/bestbets-content/master/content/${catID}.xml`;

          const scope = nock(host)
          .get(catPath)
          .replyWithFile(200, path.join(__dirname, ".", "data", `${catID}.xml`))

          const expected = await readFileAsync(path.join(__dirname, ".", "data", `${catID}.xml`), 'utf8');
          const actual = await source.getContent(host + catPath);

          expect(actual).toEqual(expected);
          expect(scope.isDone()).toBeTruthy();
        }
    );

    it('throws on 404', async () => {
      const host = 'https://raw.githubusercontent.com';
      const catPath = `/PublishedContent/BestBets/test.xml`;
      const fullPath = host + catPath;

      const scope = nock(host)
      .get(catPath)
      .reply(404)


      expect.assertions(2);
      try {
          //const actual = await source.getCategoryList();
          await source.getContent(fullPath);
      } catch (err) {
        expect(err).toMatchObject({
            message: `Could not fetch ${fullPath}`
        });
      }

      expect(scope.isDone()).toBeTruthy();
    })

    it('throws on bad category', async () => {
      const host = 'https://raw.githubusercontent.com';
      const catPath = `/PublishedContent/BestBets/test.xml`;

        const scope = nock(host)
        .get(catPath)
        .reply(200, "<test></test>")


        expect.assertions(1);
        try {
            //const actual = await source.getCategoryList();
            await source.getContent(host + catPath);
        } catch (err) {
            expect(err).toMatchObject({
                message: `Invalid BestBets Category, ${catPath}`
            });
        }

        expect(scope.isDone()).toBeTruthy();
    })
  })


  describe('mapContentToCat', () => {

    it.each([
      [
          'complex category',
          '431121'
      ],
      [
          'simple category',
          '1109313'
      ],
      [
          'cat include, no exclude',
          '1045389'
      ],
      [
          'cat exclude, no include',
          '35884'
      ]
  ])(
    'maps content %s',
    async (name, catID) => {

      // Load the content file
      const content = await readFileAsync(
        path.join(__dirname, ".", "data", `${catID}.xml`),
        'utf8'
      )

      // Load .json file as the expected
      const exectedContent = await readFileAsync(path.join(__dirname, ".", "data", `${catID}.json`), 'utf8');
      const expected = JSON.parse(exectedContent);

      const actual = await source.mapContentToCat(content);

      expect({...actual, categoryDisplay: undefined}).toEqual({...expected, categoryDisplay: undefined});
      expect(actual.categoryDisplay.replace(/\s/g,'')).toEqual(expected.categoryDisplay.replace(/\s/g,''));
      expect.assertions(2);
    })

  });

  /**
   * These are the tests for the main worker function.
   */
  describe('getRecords', () => {
    it ('gets categories', async() => {

      const api_scope = nock('https://api.github.com');

      //Setup listing download
      api_scope.get("/repos/NCIOCPL/bestbets-content/contents/content")
      .replyWithFile(
        200,
        path.join(__dirname, ".", "data", `two_item_gh_response.json`),
        { 'Content-Type': 'application/json' }
      );

      const raw_scope = nock('https://raw.githubusercontent.com');

      raw_scope.get("/NCIOCPL/bestbets-content/master/content/1045389.xml")
      .replyWithFile(200, path.join(__dirname, ".", "data", `1045389.xml`));

      raw_scope.get("/NCIOCPL/bestbets-content/master/content/1109313.xml")
      .replyWithFile(200, path.join(__dirname, ".", "data", `1109313.xml`));

      const actual = await source.getRecords();

      expect.assertions(7); // Total assertions for this function
      expect(actual).toHaveLength(2);
      for(let actcat of actual) {
        const expcat = EXPECTED_BB[actcat.categoryID];
        expect({...actcat, categoryDisplay: undefined}).toEqual({...expcat, categoryDisplay: undefined});
        expect(actcat.categoryDisplay.replace(/\s/g,'')).toEqual(expcat.categoryDisplay.replace(/\s/g,''));
      }
      expect(api_scope.isDone()).toBeTruthy();
      expect(raw_scope.isDone()).toBeTruthy();
    })

  })

  describe('cleanToString', () => {
      it('cleans string', () => {
          const actual = source.cleanToString(`
          hello
          `);
          expect(actual).toBe('hello');
      });

      it('cleans wonky string', () => {
          const actual = source.cleanToString("hello");
          expect(actual).toBe('hello');
      });

      it('cleans array', () => {
          const actual = source.cleanToString(["hello"]);
          expect(actual).toBe('hello');
      });

      it('cleans empty array', () => {
          const actual = source.cleanToString([]);
          expect(actual).toBe('');
      });

      it('cleans multi array', () => {
          const actual = source.cleanToString(["hello", "goodbye"]);
          expect(actual).toBe('hello');
      });

      it('cleans wonky array', () => {
          const actual = source.cleanToString([`
          hello
          `]);
          expect(actual).toBe('hello');
      });

      it('cleans empty', () => {
          const actual = source.cleanToString("");
          expect(actual).toBe("");
      });

      it('cleans undef', () => {
          const actual = source.cleanToString(undefined);
          expect(actual).toBe("");
      });

  })

  describe('cleanToInt', () => {
      it('cleans string', () => {
          const actual = source.cleanToInt("100");
          expect(actual).toBe(100);
      });

      it('cleans wonky string', () => {
          const actual = source.cleanToInt(`
          100
          `);
          expect(actual).toBe(100);
      });

      it('cleans array', () => {
          const actual = source.cleanToInt(["30"]);
          expect(actual).toBe(30);
      });

      it('cleans empty array', () => {
          const actual = source.cleanToInt([]);
          expect(actual).toBeNaN();
      });

      it('cleans multi array', () => {
          const actual = source.cleanToInt(["30", "20"]);
          expect(actual).toBe(30);
      });

      it('cleans wonky array', () => {
          const actual = source.cleanToInt([`
          30
          `]);
          expect(actual).toBe(30);
      });

      it('cleans empty', () => {
          const actual = source.cleanToInt("");
          expect(actual).toBeNaN();
      });

      it('cleans undef', () => {
          const actual = source.cleanToInt(undefined);
          expect(actual).toBeNaN();
      });
  })

  describe('cleanToBool', () => {
      it('cleans true', () => {
          const actual = source.cleanToBool("true");
          expect(actual).toBeTruthy();
      });

      it('cleans wonky true', () => {
          const actual = source.cleanToBool(`
          true
          `);
          expect(actual).toBeTruthy();
      });

      it('cleans true array', () => {
          const actual = source.cleanToBool(["true"]);
          expect(actual).toBeTruthy();
      });

      it('cleans empty array', () => {
          const actual = source.cleanToBool([]);
          expect(actual).not.toBeTruthy();
      });

      it('cleans wonky array', () => {
          const actual = source.cleanToBool([`
          true
          `]);
          expect(actual).toBeTruthy();
      });

      it('cleans false', () => {
          const actual = source.cleanToBool("false");
          expect(actual).not.toBeTruthy();
      });

      it('cleans empty', () => {
          const actual = source.cleanToBool("");
          expect(actual).not.toBeTruthy();
      });

      it('cleans undef', () => {
          const actual = source.cleanToBool(undefined);
          expect(actual).not.toBeTruthy();
      });
  })

  describe('getRecords', () => {

  });

  describe('begin', () => {
    it('works', async () => {
      expect.assertions(0);
      await source.begin();
    })
  })

  describe('abort', () => {
    it('works', async () => {
      expect.assertions(0);
      await source.abort();
    })
  })

  describe('end', () => {
    it('works', async () => {
      expect.assertions(0);
      await source.end();
    })
  })

  describe('ValidateConfig', () => {
    it('validates config', () => {
      const actual = GithubBestbetsSource.ValidateConfig(VALID_CONFIG);
      expect(actual).toEqual([]);
    });

    it('errors config', () => {
      const actual = GithubBestbetsSource.ValidateConfig({});
      expect(actual).toEqual([new Error("You must supply a repository URL")]);
    });

  })

  describe('GetInstance', () => {
    it('gets instance, no auth', async () => {
      const actual = await GithubBestbetsSource.GetInstance(logger, VALID_CONFIG);
      expect(actual.client).toBeTruthy();
      expect(actual.axclient).toBeTruthy();
      expect(actual.owner).toEqual("NCIOCPL");
      expect(actual.repo).toEqual("bestbets-content");
      expect(actual.contentPath).toEqual("/content");
      expect(actual.branchName).toBeFalsy();
    });


    it('gets instance, with auth', async () => {

      //NOTE: the internal git client's authenticate method does
      //nothing more then setup an object in memory that adds a
      //set of headers before every request. So we can't really check
      //that without a mock. Creating a mock for octokit and authenticate
      //will be tricky.
      const actual = await GithubBestbetsSource.GetInstance(logger, {
        ...VALID_CONFIG,
        authentication: {
          type: "token",
          token: "SECRET"
        }
      });
      expect(actual.client).toBeTruthy();
      expect(actual.axclient).toBeTruthy();
      expect(actual.owner).toEqual("NCIOCPL");
      expect(actual.repo).toEqual("bestbets-content");
      expect(actual.contentPath).toEqual("/content");
      expect(actual.branchName).toBeFalsy();
    });


    it('throws an error if config is not defined', async () => {
      try {
        const actual = await GithubBestbetsSource.GetInstance(logger);
      } catch (err) {
        expect(err).not.toBeNull();
      }
    });


  })

})
