from transformer.result import result_mapper
import pandas as pd


class TestResultMapper:
    
    class TestSuccess:
        def test_single_depth(self):
            cfg = 
    
    class TestFailure:
        pass
    def test_single_depth(self):
        config = {
            'output': {
                "mapper": "DefaultArrayResultFormatter",
                'format': [
                    {
                        'name': 'somefield',
                        'input': '{input.body.test2}'
                    },
                    {
                        'name': 'id',
                        'input': 'UuidGenerator'
                    }
                ]
            }
        }
        d = {
            'body': {'test2': [1, 2, 3, 4]}
        }
        input_data = {
            'body': pd.DataFrame(d['body'])
        }
        results = result_mapper.JsonResultMapper(config)
        assert isinstance(results, list)
        assert len(results) == 4
        assert len(results[0].keys()) == 2

    def test_depth_2_single_branch(self):
        config = {
            'output': {
                'format': {
                    'test': [
                        {
                            'name': 'somefield',
                            'input': '{input.body.test2}',
                            'validators': [
                                {
                                    "name": "",
                                    "value": ""
                                }
                            ]
                        }
                    ]
                }
            }
        }
        d = {
            'header': {'recordType': ['abcdef']},
            'body': {'test2': [1, 2, 3, 4]}
        }
        input_data = {
            'header': pd.DataFrame(d['header']),
            'body': pd.DataFrame(d['body'])
        }
        expected_results = [
            {
                'metadata': 'notimportantvalue',
                'test': {
                    'somefield': 1
                }
            },
            {
                'metadata': 'notimportantvalue',
                'test': {
                    'somefield': 2
                }
            },
            {
                'metadata': 'notimportantvalue',
                'test': {
                    'somefield': 3
                }
            },
            {
                'metadata': 'notimportantvalue',
                'test': {
                    'somefield': 4
                }
            }
        ]
        results = target.process_result(input_data, config)
        print(results)
        assert isinstance(results, dict)
        assert isinstance(results['test'], list)
        assert len(results['test']) == 4

    def test_depth_2_multiple_branch(self):
        config = {
            'output': {
                'format': {
                    'deeper': {

                        'body': {
                            'part1': [
                                {
                                    'name': 'part1Id',
                                    'input': 'UuidGenerator'
                                },
                                {
                                    'name': 'part1Data',
                                    'input': '{input.data.books}'
                                }
                            ],
                            'part2': [
                                {
                                    'name': 'part2Id',
                                    'input': 'UuidGenerator'
                                },
                                {
                                    'name': 'part2Data',
                                    'input': '{input.data.books}'
                                },
                                {
                                    'name': 'part2DataPrice',
                                    'input': '{input.data.price}'
                                }
                            ]
                        }
                    }
                }
            }
        }
        d = {
            'data': {
                'books': [
                    "Serpent Of The Mountain",
                    "Lord Of Glory",
                    "Owls Of The Sea",
                    "Traitors Of The Banished",
                    "Heirs And Descendants",
                    "Women And Robots",
                    "Revenge Of The West",
                    "Unity Of Freedom",
                    "Avoiding Nightmares",
                    "Write About The Void"
                ],
                'price': [
                    14.0,
                    15.0,
                    22.0,
                    23.0,
                    25.0,
                    26.0,
                    33.0,
                    22.0,
                    44.0,
                    5555555.0,
                ]
            }
        }
        input_data = {
            'data': pd.DataFrame(d['data'])
        }
        print()
        results = target.process_result(input_data, config)
        print(results)
        assert isinstance(results, dict)

    def test_depth_3__multiple_branch_with_metadata(self):
        config = {
            'output': {
                'format': {
                    'metadata': [
                        {
                            'name': 'batchId',
                            'input': 'UuidGenerator'
                        },
                        {
                            'name': 'correlationId',
                            'input': 'UuidGenerator'
                        },
                        {
                            'name': 'numberedId',
                            'input': 'IdGenerator'
                        }
                    ],
                    'body': {
                        'part1': [
                            {
                                'name': 'part1Id',
                                'input': 'UuidGenerator'
                            },
                            {
                                'name': 'part1Data',
                                'input': '{input.data.books}'
                            }
                        ],
                        'part2': [
                            {
                                'name': 'part2Id',
                                'input': 'UuidGenerator'
                            },
                            {
                                'name': 'part2Data',
                                'input': '{input.data.books}'
                            },
                            {
                                'name': 'part2DataPrice',
                                'input': '{input.data.price}'
                            }
                        ]
                    }
                }
            }
        }
        d = {
            'data': {
                'books': [
                    "Serpent Of The Mountain",
                    "Lord Of Glory",
                    "Owls Of The Sea",
                    "Traitors Of The Banished",
                    "Heirs And Descendants",
                    "Women And Robots",
                    "Revenge Of The West",
                    "Unity Of Freedom",
                    "Avoiding Nightmares",
                    "Write About The Void"
                ],
                'price': [
                    14.0,
                    15.0,
                    22.0,
                    23.0,
                    25.0,
                    26.0,
                    33.0,
                    22.0,
                    44.0,
                    5555555.0,
                ]
            }
        }
        input_data = {
            'data': pd.DataFrame(d['data'])
        }
        print()
        results = target.process_result(input_data, config)
        print(results)
        assert isinstance(results, dict)

    def test_depth_4__multiple_branch_with_metadata_reference(self):
        config = {
            'output': {
                'format': {
                    'metadata': [
                        {
                            'name': 'batchId',
                            'input': 'UuidGenerator'
                        },
                        {
                            'name': 'correlationId',
                            'input': 'UuidGenerator'
                        },
                        {
                            'name': 'reference',
                            'input': '{input.header.referenceId}'
                        }
                    ],
                    'body': {
                        'part1': [
                            {
                                'name': 'part1Id',
                                'input': 'UuidGenerator'
                            },
                            {
                                'name': 'part1Data',
                                'input': '{input.data.books}'
                            }
                        ],
                        'part2': [
                            {
                                'name': 'part2Id',
                                'input': 'UuidGenerator'
                            },
                            {
                                'name': 'part2Data',
                                'input': '{input.data.books}'
                            },
                            {
                                'name': 'part2DataPrice',
                                'input': '{input.data.price}'
                            }
                        ]
                    }
                }
            }
        }
        d = {
            'header': {
                'referenceId': ['SOMEUUID']
            },
            'data': {
                'books': [
                    "Serpent Of The Mountain",
                    "Lord Of Glory",
                    "Owls Of The Sea",
                    "Traitors Of The Banished",
                    "Heirs And Descendants",
                    "Women And Robots",
                    "Revenge Of The West",
                    "Unity Of Freedom",
                    "Avoiding Nightmares",
                    "Write About The Void"
                ],
                'price': [
                    14.0,
                    15.0,
                    22.0,
                    23.0,
                    25.0,
                    26.0,
                    33.0,
                    22.0,
                    44.0,
                    5555555.0,
                ]
            }
        }
        input_data = {
            'header': pd.DataFrame(d['header']),
            'data': pd.DataFrame(d['data'])
        }
        print()
        results = target.process_result(input_data, config)
        print(results)
        assert isinstance(results, dict)