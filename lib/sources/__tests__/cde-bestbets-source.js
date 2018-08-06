const CDEPublishedContentListing        = require('cde-published-content-listing');
const CDEBestbetsSource                 = require('../cde-bestbets-source');
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


/*************
 * Expected Best Bets
 **************/
const EXPECTED_BB = Object.freeze({
    "431121" : {
        categoryID: "431121",
        categoryName: "Fotos de cáncer",
        categoryWeight: 30,
        isExactMatch: true,
        language: 'es-us',
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
    }
})

const bblist = {
    "Directories": [],
    "Files": [
        {
        "FullWebPath": "/PublishedContent/BestBets/1045389.xml",
        "FileName": "1045389.xml",
        "CreationTime": "2017-08-08T11:29:24.6050474-04:00",
        "LastWriteTime": "2017-08-08T11:29:24.6060239-04:00"
        },
        {
        "FullWebPath": "/PublishedContent/BestBets/1109313.xml",
        "FileName": "1109313.xml",
        "CreationTime": "2018-03-08T09:30:14.1496335-05:00",
        "LastWriteTime": "2018-06-09T21:59:21.3337511-04:00"
        },
        {
        "FullWebPath": "/PublishedContent/BestBets/431121.xml",
        "FileName": "431121.xml",
        "CreationTime": "2017-08-08T11:29:20.8093919-04:00",
        "LastWriteTime": "2017-08-08T11:29:20.8093919-04:00"                    
        }
    ]
};


describe('CDEBestbetsSource', async () => {

    const VALID_CONFIG =  { hostname: 'www.cancer.gov' }
    const client = new CDEPublishedContentListing('www.cancer.gov');
    const source = new CDEBestbetsSource(logger, client);

    describe('getCategoryList', async () => {
        it ('gets list', async () => {
           // /PublishedContent/List?root=BestBets&fmt=json

           const scope = nock('https://www.cancer.gov')
            .get("/PublishedContent/List")
            .query({
                root: "BestBets",
                path: "/",
                fmt: "json"
                })
            .reply(200, bblist);
           
            const expected = {
                Directories: [],
                Files: [
                    {
                    "FullWebPath": "/PublishedContent/BestBets/1045389.xml",
                    "Path": [],
                    "FileName": "1045389.xml",
                    "CreationTime": "2017-08-08T11:29:24.6050474-04:00",
                    "LastWriteTime": "2017-08-08T11:29:24.6060239-04:00"
                    },
                    {
                    "FullWebPath": "/PublishedContent/BestBets/1109313.xml",
                    "FileName": "1109313.xml",
                    "Path": [],
                    "CreationTime": "2018-03-08T09:30:14.1496335-05:00",
                    "LastWriteTime": "2018-06-09T21:59:21.3337511-04:00"
                    },
                    {
                    "FullWebPath": "/PublishedContent/BestBets/431121.xml",
                    "Path": [],
                    "FileName": "431121.xml",
                    "CreationTime": "2017-08-08T11:29:20.8093919-04:00",
                    "LastWriteTime": "2017-08-08T11:29:20.8093919-04:00"                    
                    }
                ]
            };
            
            //const actual = await source.getCategoryList();
            const actual = await source.getCategoryList();

            expect(scope.isDone()).toBeTruthy();
            expect(actual).toEqual(expected);            
        })

        it ('throws error on failed listing', async () => {
            const scope = nock('https://www.cancer.gov')
            .get("/PublishedContent/List")
            .query({
                root: "BestBets",
                path: "/",
                fmt: "json"
                })
            .reply(500);

            expect.assertions(2);
            try {
                //const actual = await source.getCategoryList();
                const actual = await source.getCategoryList();
            } catch (err) {
                expect(err).toMatchObject({
                    message: 'Could not fetch Best Bets Categories List from server.'
                });
            }

            expect(scope.isDone()).toBeTruthy();            
        })
    });

    describe('getCategory', async () => {

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
                const catPath = `/PublishedContent/BestBets/${catID}.xml`;

                const scope = nock('https://www.cancer.gov')
                .get(catPath)
                .replyWithFile(200, path.join(__dirname, ".", "data", `${catID}.xml`))
                
                const actual = await source.getCategory({ FullWebPath: catPath });

                expect(actual).toEqual(EXPECTED_BB[catID]);
                expect(scope.isDone()).toBeTruthy();
            }
        );

        it('throws on 404', async () => {
            const catPath = `/PublishedContent/BestBets/test.xml`;

            const scope = nock('https://www.cancer.gov')
            .get(catPath)
            .reply(404)
            

            expect.assertions(2);
            try {
                //const actual = await source.getCategoryList();
                const actual = await source.getCategory({ FullWebPath: catPath });
            } catch (err) {
                expect(err).toMatchObject({
                    message: `Could not fetch ${catPath}`
                });
            }

            expect(scope.isDone()).toBeTruthy();  
        })

        it('throws on bad category', async () => {
            const catPath = `/PublishedContent/BestBets/test.xml`;

            const scope = nock('https://www.cancer.gov')
            .get(catPath)
            .reply(200, "<test></test>")
            

            expect.assertions(2);
            try {
                //const actual = await source.getCategoryList();
                const actual = await source.getCategory({ FullWebPath: catPath });
            } catch (err) {
                expect(err).toMatchObject({
                    message: `Invalid BestBets Category, ${catPath}`
                });
            }

            expect(scope.isDone()).toBeTruthy();  
        })

    })

    describe('getRecords', async () => {
        it ('gets categories', async() => {
            const scope = nock('https://www.cancer.gov');

            //Setup listing download
            scope.get("/PublishedContent/List")
            .query({
                root: "BestBets",
                path: "/",
                fmt: "json"
                })
            .reply(200, {
                ...bblist,
                Files: bblist.Files.slice(0,2)
            });

            scope.get("/PublishedContent/BestBets/1045389.xml")
            .replyWithFile(200, path.join(__dirname, ".", "data", `1045389.xml`))

            scope.get("/PublishedContent/BestBets/1109313.xml")
            .replyWithFile(200, path.join(__dirname, ".", "data", `1109313.xml`))

            const actual = await source.getRecords();

            expect(actual).toHaveLength(2);
            expect(actual).toContainEqual(EXPECTED_BB['1045389']);
            expect(actual).toContainEqual(EXPECTED_BB['1109313']);
            expect(scope.isDone()).toBeTruthy();
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


    describe('begin', async () => {
        it('works', async () => {
            expect.assertions(0);
            await source.begin();
        })
    })

    describe('abort', async () => {
        it('works', async () => {
            expect.assertions(0);
            await source.abort();
        })        
    })

    describe('end', async () => {
        it('works', async () => {
            expect.assertions(0);
            await source.end();
        })
    })

    describe('ValidateConfig', () => {
        it('validates config', () => {
            const actual = CDEBestbetsSource.ValidateConfig(VALID_CONFIG);
            expect(actual).toEqual([]);
        });

        it('errors config', () => {
            const actual = CDEBestbetsSource.ValidateConfig({});
            expect(actual).toEqual([new Error("You must supply a source hostname")]);
        });
    })

    describe('GetInstance', async() => {

        it('gets instance', async() => {
            const actual = await CDEBestbetsSource.GetInstance(logger, {
                hostname: 'www.cancer.gov'
            });                
            expect(actual.client).toBeTruthy();
            //TODO: Test Socket Limit       
        });

        it('gets instance with socketLimit', async() => {
            const actual = await CDEBestbetsSource.GetInstance(logger, {
                hostname: 'www.cancer.gov',
                socketLimit: 50
            });                
            expect(actual.client).toBeTruthy();
            //TODO: Test Socket Limit
        });

        it('throws an error if config is not defined', async() => {
            expect.assertions(1);
            try {
                const actual = await CDEBestbetsSource.GetInstance(logger);
            } catch (err) {
                expect(err).not.toBeNull();
            }
        });

        it('throws an error if hostname is not defined', async() => {
            expect.assertions(1);
            try {
                const actual = await CDEBestbetsSource.GetInstance(logger, {});
            } catch (err) {
                expect(err).not.toBeNull();
            }
        });

        it('throws an error for non-number socketLimit', async() => {
            expect.assertions(1);
            try {
                await CDEBestbetsSource.GetInstance(
                    logger,
                    {
                        hostname: 'www.cancer.gov',
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
                await CDEBestbetsSource.GetInstance(
                    logger,
                    {
                        hostname: 'www.cancer.gov',
                        socketLimit: -1
                    }
                );
            } catch (err) {
                expect(err).toMatchObject({
                    message: "socketLimit must be a number greater than 0"
                });
            }
        });        

    });
})