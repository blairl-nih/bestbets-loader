const CategoryToMatchTransformer        = require('../category-to-match-transformer');
const elasticsearch                     = require('elasticsearch');
const nock                              = require('nock');
const path                              = require('path');
const winston                           = require('winston');
const WinstonNullTransport              = require('winston-null-transport');

const logger = winston.createLogger({
    level: 'debug',
    format: winston.format.simple(),
    transports: [
        new WinstonNullTransport()
    ]
});

beforeAll(() => {
    nock.disableNetConnect();
})

//After each test, cleanup any remaining mocks
afterEach(() => {
    nock.cleanAll();
});

afterAll(() => {
    nock.enableNetConnect();
})

const EXAMPLE_BB = Object.freeze({
    "431121" : {
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
            { name: "imagenes de cancer", isExactMatch: true },
            { name: "imágenes de cáncer", isExactMatch: true },
            { name: "imagenes de cáncer", isExactMatch: true },
            { name: "imágenes de cancer", isExactMatch: true },
            { name: "imágenes", isExactMatch: false },
            { name: "imajenes", isExactMatch: false },
            { name: "fotografias", isExactMatch: false },
        ],
        excludeSynonyms: [
            { name: "piel", isExactMatch: false },
        ]
    },
    "1109313": {
        categoryID: "1109313",
        categoryName: "Mantle Cell Lymphoma",
        categoryWeight: 300,
        isExactMatch: false,
        language: 'en',
        display: true,
        includeSynonyms: [],
        excludeSynonyms: []
    },
    "1045389": {
        categoryID: "1045389",
        categoryName: "Cancer Research Ideas",
        categoryWeight: 100,
        isExactMatch: false,
        language: 'en',
        display: true,
        includeSynonyms: [
            { name: "Clinical Trial Ideas", isExactMatch: false }
        ],
        excludeSynonyms: []
    },
    "35884": {
        categoryID: "35884",
        categoryName: "Tobacco Control",
        categoryWeight: 110,
        isExactMatch: false,
        language: 'en',
        display: true,
        includeSynonyms: [],
        excludeSynonyms: [
            { name: "monograph", isExactMatch: false },
            { name: "Branch", isExactMatch: false }
        ]
    },
    "DUPES": {
        categoryID: "DUPES",
        categoryName: "DUPES",
        categoryWeight: 110,
        isExactMatch: false,
        language: 'en',
        display: true,
        includeSynonyms: [
            { name: "dupes", isExactMatch: false },
            { name: "not dupe", isExactMatch: false }
        ],
        excludeSynonyms: [
            { name: "dupes", isExactMatch: false },
            { name: "dupes", isExactMatch: false }
        ]
    }
})

const NAME_TOKEN_RESPONSES = {
    "": {
        "tokens": []
    },
    "dupes": {
        "tokens": [
            {
                "token": "dupe",
                "start_offset": 0,
                "end_offset": 5,
                "type": "<ALPHANUM>",
                "position": 0
            }
        ]
    },
    "not dupe": {
        "tokens": [
            {
                "token": "dupe",
                "start_offset": 4,
                "end_offset": 8,
                "type": "<ALPHANUM>",
                "position": 1
            }
        ]
    },
    "DUPES": {
        "tokens": [
            {
                "token": "dupe",
                "start_offset": 0,
                "end_offset": 5,
                "type": "<ALPHANUM>",
                "position": 0
            }
        ]
    },
    "Cancer Research Ideas": {
        "tokens": [
            {
                "token": "cancer",
                "start_offset": 0,
                "end_offset": 6,
                "type": "<ALPHANUM>",
                "position": 0
            },
            {
                "token": "research",
                "start_offset": 7,
                "end_offset": 15,
                "type": "<ALPHANUM>",
                "position": 1
            },
            {
                "token": "idea",
                "start_offset": 16,
                "end_offset": 21,
                "type": "<ALPHANUM>",
                "position": 2
            }
        ]
    },
    "Clinical Trial Ideas": {
        "tokens": [
            {
                "token": "clinical",
                "start_offset": 0,
                "end_offset": 8,
                "type": "<ALPHANUM>",
                "position": 0
            },
            {
                "token": "trial",
                "start_offset": 9,
                "end_offset": 14,
                "type": "<ALPHANUM>",
                "position": 1
            },
            {
                "token": "idea",
                "start_offset": 15,
                "end_offset": 20,
                "type": "<ALPHANUM>",
                "position": 2
            }
        ]
    },
    "colo-rectal cancers": {
        "tokens": [
            {
                "token": "colo",
                "start_offset": 0,
                "end_offset": 4,
                "type": "<ALPHANUM>",
                "position": 0
            },
            {
                "token": "rectal",
                "start_offset": 5,
                "end_offset": 11,
                "type": "<ALPHANUM>",
                "position": 1
            },
            {
                "token": "cancer",
                "start_offset": 12,
                "end_offset": 19,
                "type": "<ALPHANUM>",
                "position": 2
            }
        ]
    },
    "Test": {
        "tokens": [ { "token": "test", "start_offset": 0, "end_offset": 4,
            "type": "<ALPHANUM>", "position": 0 }]
    },
    "Tobacco Control": {
        "tokens": [
            {
                "token": "tobacco",
                "start_offset": 0,
                "end_offset": 7,
                "type": "<ALPHANUM>",
                "position": 0
            },
            {
                "token": "control",
                "start_offset": 8,
                "end_offset": 15,
                "type": "<ALPHANUM>",
                "position": 1
            }
        ]
    },
    "monograph": {
        "tokens": [
            {
                "token": "monograph",
                "start_offset": 0,
                "end_offset": 9,
                "type": "<ALPHANUM>",
                "position": 0
            }
        ]
    },
    "Branch": {
        "tokens": [
            {
                "token": "branch",
                "start_offset": 0,
                "end_offset": 6,
                "type": "<ALPHANUM>",
                "position": 0
            }
        ]
    }

}

const elasticClient = new elasticsearch.Client({
    hosts: ['http://example.org:9200'],
    apiVersion: '5.6',
    maxSockets: 100,
    keepAlive: true
});

const GOOD_CON_CONFIG = {
    "tokenizer": "standard",
    "filter": [
        "lowercase",
        {
            "type": "stop",
            "stopwords": ["a", "an", "and", "are", "as", "at", "be", "but", "by",
                "for", "if", "in", "into", "is", "it",
                "not", "of", "on", "or", "such",
                "that", "the", "their", "then", "there", "these",
                "they", "this", "to", "was", "will", "with"
            ]
        },
        "unique",
        {
            "type": "stemmer",
            "language": "minimal_english"
        }
    ]
}

const GOOD_STEP_CONFIG = {
    "eshosts": [ "http://example.org:9200" ],
    "settingsPath": "es-mappings/settings.json",
    "analyzer": "nostem"
}


const xformer = new CategoryToMatchTransformer(logger, elasticClient, GOOD_CON_CONFIG);

/**
 * This is a helper function to add a name token call to a nock
 * @param {*} scope the nock.
 * @param {*} name the name
 */
function addNameTokenToNock(scope, name){
    scope.post("/_analyze", (body) => body["text"] === name)
            .reply(200, NAME_TOKEN_RESPONSES[name]);
}


describe('CategoryToMatchTransformer', async () => {

    describe('Constructor', () => {
        it('works as normal', () => {
            const xformer = new CategoryToMatchTransformer(logger, elasticClient, GOOD_CON_CONFIG);
            expect(xformer.esclient).toBe(elasticClient);
            expect(xformer.analyzerSetting).toEqual(GOOD_CON_CONFIG);
        })

        it('sets analyzer to empty', () => {
            const xformer = new CategoryToMatchTransformer(logger, elasticClient, {});
            expect(xformer.analyzerSetting).toEqual({});
        })

        it('sets analyzer to only tokenizer', () => {
            const xformer = new CategoryToMatchTransformer(logger, elasticClient, {
                tokenizer: GOOD_CON_CONFIG.tokenizer
            });
            expect(xformer.analyzerSetting).toEqual({tokenizer: GOOD_CON_CONFIG.tokenizer});
        })

        it('sets analyzer to only filter', () => {
            const xformer = new CategoryToMatchTransformer(logger, elasticClient, {
                filter: GOOD_CON_CONFIG.filter
            });
            expect(xformer.analyzerSetting).toEqual({filter: GOOD_CON_CONFIG.filter});
        })

        it('throws on missing esclient', () => {
            expect(() => {
                new CategoryToMatchTransformer(logger);
            }).toThrow("You must supply an Elasticsearch client");
        })

    })


    describe('extractNamesToTokenize', () => {

        it.each([
            [
                'complex terms',
                '431121',
                [ "fotos", "imagenes", "fotos de cancer", "imagenes de cancer",
                    "imágenes de cáncer", "imagenes de cáncer", "imágenes de cancer",
                    "imágenes", "imajenes", "fotografias", "piel", "Fotos de cáncer" ]
            ],
            [
                'no syns',
                '1109313',
                ['Mantle Cell Lymphoma']
            ],
            [
                'only include syn',
                '1045389',
                ['Cancer Research Ideas', 'Clinical Trial Ideas']
            ],
            [
                'only exclude syn',
                '35884',
                ['Tobacco Control', 'Branch', 'monograph' ]
            ],
            [
                'handles dupes',
                'DUPES',
                ['DUPES', 'not dupe']
            ]
        ])(
            'handles %s',
            (name, catID, expected) => {
                const actual = xformer.extractNamesToTokenize(EXAMPLE_BB[catID]);
                expect(actual.sort()).toEqual(expected.sort());
            }
        )
    })

    describe('tokenizeNames', async() => {

        it('tokenizes list', async () => {
            const list = [ "Cancer Research Ideas", "Clinical Trial Ideas" ];

            //Setup nock and es
            const scope = nock('http://example.org:9200');
            list.forEach(addNameTokenToNock.bind(this, scope));

            const actual = await xformer.tokenizeNames(list);
            expect(actual).toEqual({
                "Cancer Research Ideas": 3,
                "Clinical Trial Ideas": 3
            })
            expect(scope.isDone()).toBeTruthy();
        })

    })

    describe('tokenizeName', async () => {

        it.each([
            ['simple string', "Clinical Trial Ideas", 3],
            ['empty string', "", 0],
            ['complex string', "colo-rectal cancers", 3]
        ])('tokenizes %s', async (testName, name, expected) => {
            const scope = nock('http://example.org:9200');
            //Setup listing download
            addNameTokenToNock(scope, name);

            const actual = await xformer.tokenizeName(name);
            expect(actual).toBe(expected);
            expect(scope.isDone()).toBeTruthy();
        })

        it('throws error on failure', async () => {

            const scope = nock('http://example.org:9200');
            //Setup listing download
            scope.post("/_analyze", (body) => body["text"] === "error")
            .reply(500);

            expect.assertions(2);

            try {
                await xformer.tokenizeName("error");
            } catch (err) {
                expect(err).toMatchObject({
                    message: 'Internal Server Error'
                });
            }

            expect(scope.isDone()).toBeTruthy();
        })

    })

    describe('tokenizeNameWrap', async () => {

        it ('fetches a term normally', async () => {

            const xformer2 = new CategoryToMatchTransformer(logger, elasticClient, GOOD_CON_CONFIG);

            const scope = nock('http://example.org:9200');
            //Setup listing download
            addNameTokenToNock(scope, "Test");

            const actual = await xformer2.tokenizeNameWrap("Test");
            expect(actual).toBe(1);
            expect(xformer2.tokenCache["Test".toLowerCase()]).toBe(1);
            expect(scope.isDone()).toBeTruthy();
        })

        it ('fetches a term only once', async () => {

            const xformer2 = new CategoryToMatchTransformer(logger, elasticClient, GOOD_CON_CONFIG);

            const scope = nock('http://example.org:9200');
            //Setup listing download
            scope.post("/_analyze", (body) => body["text"] === "Test")
            .delay(500)
            .reply(200, NAME_TOKEN_RESPONSES["Test"]);

            const actual = await Promise.all([
                xformer2.tokenizeNameWrap("Test"),
                xformer2.tokenizeNameWrap("Test")
            ]);
            expect(actual).toEqual([1,1]);

            //If this is done and no exceptions are thrown, then only
            //one call has occurred.
            expect(scope.isDone()).toBeTruthy();
        })

        it ('fetches a term only once, different case', async () => {

            const xformer2 = new CategoryToMatchTransformer(logger, elasticClient, GOOD_CON_CONFIG);

            const scope = nock('http://example.org:9200');
            //Setup listing download
            scope.post("/_analyze", (body) => body["text"] === "Test")
            .delay(500)
            .reply(200, NAME_TOKEN_RESPONSES["Test"]);

            const actual = await Promise.all([
                xformer2.tokenizeNameWrap("Test"),
                xformer2.tokenizeNameWrap("TEST")
            ]);
            expect(actual).toEqual([1,1]);

            //If this is done and no exceptions are thrown, then only
            //one call has occurred.
            expect(scope.isDone()).toBeTruthy();
        })
    });


    describe('transform', async () => {

        it('transforms no exclude', async () => {
            const scope = nock('http://example.org:9200');
            //Setup listing download
            addNameTokenToNock(scope, "Cancer Research Ideas");
            addNameTokenToNock(scope, "Clinical Trial Ideas");

            const expected = [
                {
                  isCategory: true,
                    category: EXAMPLE_BB["1045389"].categoryName,
                    weight: EXAMPLE_BB["1045389"].categoryWeight,
                    contentID: EXAMPLE_BB["1045389"].categoryID,
                    synonym: EXAMPLE_BB["1045389"].categoryName,
                    language: EXAMPLE_BB["1045389"].language,
                    isNegated: false,
                    isExact: EXAMPLE_BB["1045389"].isExactMatch,
                    tokenCount: 3
                },
                {
                  isCategory: false,
                    category: EXAMPLE_BB["1045389"].categoryName,
                    weight: EXAMPLE_BB["1045389"].categoryWeight,
                    contentID: EXAMPLE_BB["1045389"].categoryID,
                    synonym: EXAMPLE_BB["1045389"].includeSynonyms[0].name,
                    language: EXAMPLE_BB["1045389"].language,
                    isNegated: false,
                    isExact: EXAMPLE_BB["1045389"].includeSynonyms[0].isExactMatch,
                    tokenCount: 3
                }
            ];

            const actual = await xformer.transform(EXAMPLE_BB["1045389"]);

            expect(actual).toEqual(expected);
        });

        it('transforms no includes', async () => {
            const scope = nock('http://example.org:9200');
            //Setup listing download
            addNameTokenToNock(scope, "Tobacco Control");
            addNameTokenToNock(scope, "monograph");
            addNameTokenToNock(scope, "Branch");

            const expected = [
                {
                  isCategory: true,
                    category: EXAMPLE_BB["35884"].categoryName,
                    weight: EXAMPLE_BB["35884"].categoryWeight,
                    contentID: EXAMPLE_BB["35884"].categoryID,
                    synonym: EXAMPLE_BB["35884"].categoryName,
                    language: EXAMPLE_BB["35884"].language,
                    isNegated: false,
                    isExact: EXAMPLE_BB["35884"].isExactMatch,
                    //no weight?
                    tokenCount: 2
                },
                {
                  isCategory: false,
                    category: EXAMPLE_BB["35884"].categoryName,
                    weight: EXAMPLE_BB["35884"].categoryWeight,
                    contentID: EXAMPLE_BB["35884"].categoryID,
                    synonym: EXAMPLE_BB["35884"].excludeSynonyms[0].name,
                    language: EXAMPLE_BB["35884"].language,
                    isNegated: true,
                    isExact: EXAMPLE_BB["35884"].excludeSynonyms[0].isExactMatch,
                    tokenCount: 1
                },
                {
                  isCategory: false,
                    category: EXAMPLE_BB["35884"].categoryName,
                    weight: EXAMPLE_BB["35884"].categoryWeight,
                    contentID: EXAMPLE_BB["35884"].categoryID,
                    synonym: EXAMPLE_BB["35884"].excludeSynonyms[1].name,
                    language: EXAMPLE_BB["35884"].language,
                    isNegated: true,
                    isExact: EXAMPLE_BB["35884"].excludeSynonyms[1].isExactMatch,
                    tokenCount: 1
                }
            ];

            const actual = await xformer.transform(EXAMPLE_BB["35884"]);

            expect(actual).toEqual(expected);
        });
    });


    describe('begin', async () => {
        it('works', async () => {
            expect.assertions(0);
            await xformer.begin();
        })
    })

    describe('abort', async () => {
        it('works', async () => {
            expect.assertions(0);
            await xformer.abort();
        })
    })

    describe('end', async () => {
        it('works', async () => {
            expect.assertions(0);
            await xformer.end();
        })
    })


    describe('ValidateConfig', () => {

        it.each([
            ['has no errors', GOOD_STEP_CONFIG, []],
            [
                'has error on no es hosts',
                {
                    ...GOOD_STEP_CONFIG,
                    eshosts: undefined
                },
                [
                    new Error("eshosts is required for the transformer")
                ]
            ],
            [
                'has error on no settings',
                {
                    ...GOOD_STEP_CONFIG,
                    settingsPath: undefined
                },
                [
                    new Error("settingsPath is required for the transformer")
                ]
            ],
            [
                'has error on no analyzer',
                {
                    ...GOOD_STEP_CONFIG,
                    analyzer: undefined
                },
                [
                    new Error("You must supply the name of analyzer defined in the settings")
                ]
            ]
        ])(
            '%s',
            (name, config, expected) => {
                const actual = CategoryToMatchTransformer.ValidateConfig(config);
                expect(actual).toEqual(expected);
            }
        )
    })

    describe('GetInstance', async () => {
        it('works', async () => {
            const expected = new CategoryToMatchTransformer(logger, elasticClient, GOOD_CON_CONFIG);
            const actual = await CategoryToMatchTransformer.GetInstance(logger, GOOD_STEP_CONFIG);
            expect(actual.analyzerSetting).toEqual(expected.analyzerSetting);
            expect(actual.esclient.transport.hosts).toEqual(expected.esclient.transport.hosts)
        })

        it('throws an error on no eshosts', async() => {
            expect.assertions(1);
            try {
                await CategoryToMatchTransformer.GetInstance(
                    logger,
                    {
                        ...GOOD_STEP_CONFIG,
                        eshosts: undefined
                    }
                );
            } catch (err) {
                expect(err).toMatchObject({
                    message: "eshosts is required for the elastic loader"
                });
            }
        });

        it('throws an error on no settings', async() => {
            expect.assertions(1);
            try {
                await CategoryToMatchTransformer.GetInstance(
                    logger,
                    {
                        ...GOOD_STEP_CONFIG,
                        settingsPath: undefined
                    }
                );
            } catch (err) {
                expect(err).toMatchObject({
                    message: "settingsPath is required for the elastic loader"
                });
            }
        });

        it('throws an error on non-string settings', async() => {
            expect.assertions(1);
            try {
                await CategoryToMatchTransformer.GetInstance(
                    logger,
                    {
                        ...GOOD_STEP_CONFIG,
                        settingsPath: []
                    }
                );
            } catch (err) {
                expect(err).toMatchObject({
                    message: "settingsPath is required for the elastic loader"
                });
            }
        });

        it('throws an error on bad settings path', async() => {
            const fullPath = path.join(__dirname, '../../../', 'chicken')
            expect.assertions(1);
            try {
                await CategoryToMatchTransformer.GetInstance(
                    logger,
                    {
                        ...GOOD_STEP_CONFIG,
                        settingsPath: 'chicken'
                    }
                );
            } catch (err) {
                expect(err).toMatchObject({
                    message: `settingsPath cannot be loaded: ${fullPath}`
                });
            }
        });


        it('throws an error on no analyzer', async() => {
            expect.assertions(1);
            try {
                await CategoryToMatchTransformer.GetInstance(
                    logger,
                    {
                        ...GOOD_STEP_CONFIG,
                        analyzer: undefined
                    }
                );
            } catch (err) {
                expect(err).toMatchObject({
                    message: "You must supply the name of analyzer defined in the settings"
                });
            }
        });

        it('throws an error on non-string analyzer', async() => {
            expect.assertions(1);
            try {
                await CategoryToMatchTransformer.GetInstance(
                    logger,
                    {
                        ...GOOD_STEP_CONFIG,
                        analyzer: []
                    }
                );
            } catch (err) {
                expect(err).toMatchObject({
                    message: "You must supply the name of analyzer defined in the settings"
                });
            }
        });

        it('throws an error when analyzer not found', async() => {
            expect.assertions(1);
            try {
                await CategoryToMatchTransformer.GetInstance(
                    logger,
                    {
                        ...GOOD_STEP_CONFIG,
                        analyzer: 'chicken'
                    }
                );
            } catch (err) {
                expect(err).toMatchObject({
                    message: "Could not find analyzer in settings"
                });
            }
        });

        it('throws an error for non-number socketLimit', async() => {
            expect.assertions(1);
            try {
                await CategoryToMatchTransformer.GetInstance(
                    logger,
                    {
                        ...GOOD_STEP_CONFIG,
                        socketLimit: 'chicken'
                    }
                );
            } catch (err) {
                expect(err).toMatchObject({
                    message: "socketLimit must be a number greater than 0"
                });
            }
        });

        it('throws an error for negative socketLimit', async() => {
            expect.assertions(1);
            try {
                await CategoryToMatchTransformer.GetInstance(
                    logger,
                    {
                        ...GOOD_STEP_CONFIG,
                        socketLimit: -1
                    }
                );
            } catch (err) {
                expect(err).toMatchObject({
                    message: "socketLimit must be a number greater than 0"
                });
            }
        });


    })

})
