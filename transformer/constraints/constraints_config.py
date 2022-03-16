import typing
from dataclasses import dataclass
import transformer.library.exceptions as exceptions


@dataclass
class FieldCheckRule:
    source: str
    ref: str


@dataclass
class SegmentCheckRule:
    source_segment: str
    ref_segment: str
    source_aggregate: str
    ref_aggregate: str
    fields: typing.List[FieldCheckRule]

    def __init__(self, segment: dict):
        if not segment:
            raise exceptions.ConstraintMisconfigurationException('Provided segment config is empty!')
        self.source_segment = segment['source_segment']
        self.ref_segment = segment['ref_segment']
        self.source_aggregate = segment['source_aggregate'] if 'source_aggregate' in segment.keys() else ''
        self.ref_aggregate = segment['ref_aggregate'] if 'ref_aggregate' in segment.keys() else ''
        if (len(self.source_aggregate) == 0 and len(self.ref_aggregate) > 0) or(len(self.source_aggregate) > 0 and len(self.ref_aggregate) == 0):
            raise exceptions.ConstraintMisconfigurationException(f'Field {self.source_segment}:{self.ref_segment} source_aggregate or ref_aggregate. Both must be provided.')

        self.fields = []
        for field in segment['fields']:
            self.fields.append(FieldCheckRule(field['source'], field['ref']))

    def get_source_field_array(self):
        if self.has_aggregate():
            results = [self.source_aggregate]
        else:
            results = []
        for field in self.fields:
            results.append(field.source)
        return results

    def get_ref_field_array(self):
        if self.has_aggregate():
            results = [self.ref_aggregate]
        else:
            results = []
        for field in self.fields:
            results.append(field.ref)
        return results

    def has_aggregate(self):
        if self.source_aggregate == '' and self.ref_aggregate == '':
            return False
        return True
