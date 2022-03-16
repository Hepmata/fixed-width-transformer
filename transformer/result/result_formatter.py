from typing import Dict, List

import pandas as pd
import transformer.result.generator as generator
from transformer.result.result_config import ResultFormatterConfig, ResultFieldFormat


class AbstractResultFormatter:
    def prepare(
        self, config: ResultFormatterConfig, frames: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        pass

    def transform(self, frames: Dict[str, pd.DataFrame]) -> list:
        pass

    def run(self, config: dict, frames: Dict[str, pd.DataFrame]):
        pass


class DefaultArrayResultFormatter(AbstractResultFormatter):
    def prepare(
        self, config: ResultFormatterConfig, frames: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        if not config.formats:
            return frames
        ndf = {}
        for segment in config.formats:
            max_count = find_max_count(segment, config, frames)
            segment_data = []
            for fmt in config.formats[segment]:
                if '.' in fmt.value:
                    splits = fmt.value.split('.')
                    if len(frames[splits[-2]][splits[-1]].index) < max_count:
                        elements = []
                        [
                            elements.append(frames[splits[-2]][splits[-1]].values[0])
                            for x in range(max_count)
                        ]
                        segment_data.append(pd.DataFrame({fmt.name: elements}))
                    else:
                        segment_data.append(
                            pd.DataFrame({fmt.name: frames[splits[-2]][splits[-1]]})
                        )
                else:
                    generated = getattr(generator, fmt.value)().run_multiple(max_count)
                    segment_data.append(pd.DataFrame({fmt.name: generated}))

            ndf[segment] = pd.concat(segment_data, axis=1)
        return ndf

    def transform(self, frames: Dict[str, pd.DataFrame]) -> list:
        results = []
        max_frame_count = find_max_segment_count(frames)
        if len(frames) == 1 and 'root' in frames.keys():
            results = frames['root'].to_dict('records')
            return results
        else:
            for f in frames:
                multiplier = (
                    max_frame_count if len(frames[f].index) < max_frame_count else 1
                )
                results.append(
                    pd.DataFrame({f: frames[f].to_dict('records') * multiplier})
                )
            return pd.concat(results, axis=1).to_dict('records')


def find_max_count(segment, config, frames):
    max_count = 1
    for fmt in config.formats[segment]:
        if '.' in fmt.value:
            splits = fmt.value.split('.')
            current_count = len(frames[splits[-2]].index)
            if current_count > max_count:
                max_count = current_count

    return max_count


def find_max_segment_count(frames):
    max_count = 1
    for f in frames:
        current_count = len(frames[f].index)
        if current_count > max_count:
            max_count = current_count
    return max_count


# class JsonArrayResultMapper(AbstractResultFormatter):
#     """
#     This ResultMapper maps incoming dataframes and returns it as list
#     """
#     def run(self, config:ResultFormatterConfig, input_data: dict[pd.DataFrame]):
#         return self.__list__(self.recursion(input_data, config))
#
#     def __list__(self, dfs):
#         print(dfs)
#         max_count = 1
#         if isinstance(dfs, list):
#             return dfs
#         for df in dfs:
#             count = len(dfs[df])
#             if isinstance(dfs[df], dict):
#                 continue
#             else:
#                 if count > max_count:
#                     max_count = count
#
#         for df in dfs:
#             if len(dfs[df]) < max_count:
#                 dfs[df] = dfs[df] * max_count
#
#         final_result = []
#         for itr in range(max_count):
#             data = {}
#             for df in dfs:
#                 data[df] = dfs[df][itr]
#             final_result.append(data)
#         return final_result
#
#     def recursion(self, input_data, node):
#         if isinstance(node, list):
#             print("Deepest depth reached! Data: {}".format(node))
#             columns = []
#             to_generate = []
#             has_reference_field = False
#             for n in node:
#                 if 'input' in n['input']:
#                     has_reference_field = True
#             if has_reference_field:
#                 target_segment = ""
#                 for n in node:
#                     if 'input' in n['input']:
#                         splits = n['input'].replace("{", "").replace("}", "").split('.')
#                         target_segment = splits[-2]
#                         columns.append(splits[-1])
#                     else:
#                         to_generate.append(n)
#                 column_count = len(input_data[target_segment].index)
#                 selected = input_data[target_segment][columns]
#                 to_append = {}
#                 for g in to_generate:
#                     to_append[g['name']] = getattr(generator, g['input'])().run_multiple(column_count)
#                 to_append_df = pd.DataFrame(to_append)
#                 concatenated = pd.concat([selected, to_append_df], axis=1)
#                 return concatenated.to_dict('records')
#             else:
#                 # This segment is for data that has no reference, meaning its a straight up generation type.
#                 to_append = {}
#                 for n in node:
#                     to_append[n['name']] = getattr(generator, n['input'])().run()
#                 return [to_append]
#         else:
#             if len(node.keys()) == 1:
#                 for depth in node:
#                     return {depth: self.recursion(input_data, node[depth])}
#             else:
#                 results = {}
#                 for depth in node:
#                     results[depth] = self.recursion(input_data, node[depth])
#                 return results
#
#
# class JsonResultMapper(AbstractResultFormatter):
#     def run(self, config:ResultFormatterConfig, input_data: dict[pd.DataFrame]):
#         return self.recursion(input_data, config)
#
#     def recursion(self, input_data, node):
#         if isinstance(node, list):
#             print("Deepest depth reached! Data: {}".format(node))
#             columns = []
#             to_generate = []
#             has_reference_field = False
#             for n in node:
#                 if 'input' in n['input']:
#                     has_reference_field = True
#             if has_reference_field:
#                 target_segment = ""
#                 for n in node:
#                     if 'input' in n['input']:
#                         splits = n['input'].replace("{", "").replace("}", "").split('.')
#                         target_segment = splits[-2]
#                         columns.append(splits[-1])
#                     else:
#                         to_generate.append(n)
#                 column_count = len(input_data[target_segment].index)
#                 selected = input_data[target_segment][columns]
#                 to_append = {}
#                 for g in to_generate:
#                     to_append[g['name']] = getattr(generator, g['input'])().run_multiple(column_count)
#                 to_append_df = pd.DataFrame(to_append)
#                 concatenated = pd.concat([selected, to_append_df], axis=1)
#                 return concatenated.to_dict('records')
#             else:
#                 # This segment is for data that has no reference, meaning its a straight up generation type.
#                 to_append = {}
#                 for n in node:
#                     to_append[n['name']] = getattr(generator, n['input'])().run()
#                 return to_append
#         else:
#             print(node)
#             if len(node.keys()) == 1:
#                 for depth in node:
#                     return {depth: self.recursion(input_data, node[depth])}
#             else:
#                 results = {}
#                 for depth in node:
#                     results[depth] = self.recursion(input_data, node[depth])
#                 return results
