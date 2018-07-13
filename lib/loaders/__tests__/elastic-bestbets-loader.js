const ElasticBestbetsLoader             = require('../elastic-bestbets-loader');
const elasticsearch                     = require('elasticsearch');
const moment                            = require('moment');
const nock                              = require('nock');
const path                              = require('path');
const winston                           = require('winston');
const WinstonNullTransport              = require('winston-null-transport');
const ElasticTools                      = require('elastic-tools')
jest.mock('elastic-tools');


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

beforeEach(() => {
    ElasticTools.mockClear();
})

//After each test, cleanup any remaining mocks
afterEach(() => {
    nock.cleanAll();
});

afterAll(() => {
    nock.enableNetConnect();
})

const GOOD_CON_CONFIG = {
    "daysToKeep": 10,               
    "aliasName": "bestbets_v1"
}

const GOOD_STEP_CONFIG = {
    "eshosts": [ "http://localhost:9200" ],
    "daysToKeep": 10,               
    "aliasName": "bestbets_v1",
    "mappingPath": "es-mappings/mappings.json",
    "settingsPath": "es-mappings/settings.json"
 }


const elasticClient = new elasticsearch.Client({
    hosts: ['http://example.org:9200'],
    apiVersion: '5.6',
    maxSockets: 100,
    keepAlive: true
});

describe('ElasticBestbetsLoader', async () => {
    describe('constructor', () => {

    });

    describe('begin', async () => {

    })

    describe('abort', async () => {
        
    })

    describe('end', async () => {
        
    })

    describe('loadRecord', async () => {

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
                    new Error("eshosts is required for the elastic loader")
                ]
            ],
            [
                'has error on no settings',
                {
                    ...GOOD_STEP_CONFIG,
                    settingsPath: undefined
                },
                [
                    new Error("settingsPath is required for the elastic loader")
                ]
            ],
            [
                'has error on no mappings',
                {
                    ...GOOD_STEP_CONFIG,
                    mappingPath: undefined
                },
                [
                    new Error("mappingPath is required for the elastic loader")
                ]
            ]
        ])(
            '%s',
            (name, config, expected) => {
                const actual = ElasticBestbetsLoader.ValidateConfig(config);
                expect(actual).toEqual(expected);
            }
        )
    })

    describe('GetInstance', async () => {
        it('works', async () => {
            const actual = await ElasticBestbetsLoader.GetInstance(logger, GOOD_STEP_CONFIG);
            expect(actual.aliasName).toEqual(GOOD_CON_CONFIG.aliasName);
            expect(actual.daysToKeep).toEqual(GOOD_CON_CONFIG.daysToKeep);
            expect(actual.minIndexesToKeep).toEqual(2);
            expect(actual.estools.client.transport._config.hosts).toEqual(GOOD_STEP_CONFIG.eshosts);
        })

        it('throws an error on no eshosts', async() => {
            expect.assertions(1);
            try {
                await ElasticBestbetsLoader.GetInstance(
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
                await ElasticBestbetsLoader.GetInstance(
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
                await ElasticBestbetsLoader.GetInstance(
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
                await ElasticBestbetsLoader.GetInstance(
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


        it('throws an error on no aliasName', async() => {
            expect.assertions(1);
            try {
                await ElasticBestbetsLoader.GetInstance(
                    logger,
                    {
                        ...GOOD_STEP_CONFIG,
                        aliasName: undefined
                    }
                );
            } catch (err) {
                expect(err).toMatchObject({
                    message: "aliasName is required for the elastic loader"
                });
            }
        });

        it('throws an error on non-string aliasName', async() => {
            expect.assertions(1);
            try {
                await ElasticBestbetsLoader.GetInstance(
                    logger,
                    {
                        ...GOOD_STEP_CONFIG,
                        aliasName: []
                    }
                );
            } catch (err) {
                expect(err).toMatchObject({
                    message: "aliasName is required for the elastic loader"
                });
            }
        });
 
    })


})