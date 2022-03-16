import pytest
import yaml

import transformer.constraints.constraints_config as config
import transformer.library.exceptions as exceptions


class TestConstraintConfig:
    def test_successful_mapping_without_aggregate(self, mocker):
        arguments_text = """
        config:
        - source_segment: body
          ref_segment: b
          fields:
          - source: name
            ref: name
          - source: amount
            ref: amount
        """
        arguments = yaml.safe_load(arguments_text)
        for segment in arguments['config']:
            segment_config = config.SegmentCheckRule(segment)
            assert segment_config.source_segment == segment['source_segment']
            assert segment_config.ref_segment == segment['ref_segment']
            assert segment_config.has_aggregate() is False
            assert segment_config.source_aggregate == ''
            assert segment_config.ref_aggregate == ''
            assert len(segment_config.get_ref_field_array()) == 2
            assert len(segment_config.get_source_field_array()) == 2

    def test_successful_mapping_without_aggregate_multiple(self, mocker):
        arguments_text = """
        config:
        - source_segment: body
          ref_segment: b
          fields:
          - source: name
            ref: name
          - source: amount
            ref: amount
        - source_segment: body
          ref_segment: b
          fields:
          - source: name
            ref: name
          - source: amount
            ref: amount
        """
        arguments = yaml.safe_load(arguments_text)
        segments = []
        for segment in arguments['config']:
            segment_config = config.SegmentCheckRule(segment)
            segments.append(segment_config)
            assert segment_config.source_segment == segment['source_segment']
            assert segment_config.ref_segment == segment['ref_segment']
            assert segment_config.has_aggregate() is False
            assert segment_config.source_aggregate == ''
            assert segment_config.ref_aggregate == ''
            assert len(segment_config.get_ref_field_array()) == 2
            assert len(segment_config.get_source_field_array()) == 2
        assert len(segments) == len(arguments['config'])

    def test_successful_mapping_with_aggregate(self, mocker):
        arguments_text = """
        config:
        - source_segment: body
          ref_segment: b
          source_aggregate: a
          ref_aggregate: b
          fields:
          - source: name
            ref: name
          - source: amount
            ref: amount
        """
        arguments = yaml.safe_load(arguments_text)
        for segment in arguments['config']:
            segment_config = config.SegmentCheckRule(segment)
            assert segment_config.source_segment == segment['source_segment']
            assert segment_config.ref_segment == segment['ref_segment']
            assert segment_config.has_aggregate() is True
            assert segment_config.source_aggregate == segment['source_aggregate']
            assert segment_config.ref_aggregate == segment['ref_aggregate']
            assert len(segment_config.get_ref_field_array()) == 3
            assert len(segment_config.get_source_field_array()) == 3

    def test_successful_mapping_with_aggregate_multiple(self, mocker):
        arguments_text = """
        config:
        - source_segment: body
          ref_segment: b
          source_aggregate: a
          ref_aggregate: b
          fields:
          - source: name
            ref: name
          - source: amount
            ref: amount
        - source_segment: body
          ref_segment: b
          source_aggregate: a
          ref_aggregate: b
          fields:
          - source: name
            ref: name
          - source: amount
            ref: amount
        """
        arguments = yaml.safe_load(arguments_text)
        segments = []
        for segment in arguments['config']:
            segment_config = config.SegmentCheckRule(segment)
            segments.append(segment_config)
            assert segment_config.source_segment == segment['source_segment']
            assert segment_config.ref_segment == segment['ref_segment']
            assert segment_config.has_aggregate() is True
            assert segment_config.source_aggregate == segment['source_aggregate']
            assert segment_config.ref_aggregate == segment['ref_aggregate']
            assert len(segment_config.get_ref_field_array()) == 3
            assert len(segment_config.get_source_field_array()) == 3
        assert len(segments) == len(arguments['config'])

    def test_successful_mapping_mixed_multiple(self, mocker):
        arguments_text = """
        config:
        - source_segment: body
          ref_segment: b
          source_aggregate: a
          ref_aggregate: b
          fields:
          - source: name
            ref: name
          - source: amount
            ref: amount
        - source_segment: body
          ref_segment: b
          fields:
          - source: name
            ref: name
          - source: amount
            ref: amount
        """
        arguments = yaml.safe_load(arguments_text)
        segments = []
        for segment in arguments['config']:
            segment_config = config.SegmentCheckRule(segment)
            segments.append(segment_config)
            assert segment_config.source_segment == segment['source_segment']
            assert segment_config.ref_segment == segment['ref_segment']
            state = True if 'source_aggregate' in segment.keys() else False
            assert segment_config.has_aggregate() is state
            state = segment['source_aggregate'] if 'source_aggregate' in segment.keys() else ''
            assert segment_config.source_aggregate == state
            state = segment['ref_aggregate'] if 'ref_aggregate' in segment.keys() else ''
            assert segment_config.ref_aggregate == state
            state = len(segment['fields']) if 'ref_aggregate' not in segment.keys() else len(segment['fields']) + 1
            assert len(segment_config.get_ref_field_array()) == state
            state = len(segment['fields']) if 'source_aggregate' not in segment.keys() else len(segment['fields']) + 1
            assert len(segment_config.get_source_field_array()) == state
        assert len(segments) == len(arguments['config'])

    def test_failure_missing_one_aggregate(self):
        arguments_text = """
        config:
        - source_segment: body
          ref_segment: b
          source_aggregate: a
          fields:
          - source: name
            ref: name
          - source: amount
            ref: amount
        """
        arguments = yaml.safe_load(arguments_text)
        for segment in arguments['config']:
            with pytest.raises(exceptions.ConstraintMisconfigurationException):
                config.SegmentCheckRule(segment)

    def test_empty_config(self):
        with pytest.raises(exceptions.ConstraintMisconfigurationException):
            config.SegmentCheckRule({})
