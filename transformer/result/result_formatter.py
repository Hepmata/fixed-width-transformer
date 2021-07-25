from typing import Dict, List

import pandas as pd
import transformer.result.generator as generator
from transformer.result.result_config import ResultFormatterConfig, ResultFieldFormat


class AbstractResultFormatter:
    def run(self, config:dict, frames: Dict[str, pd.DataFrame]): pass


class DefaultArrayResultFormatter(AbstractResultFormatter):
    def run(self, config: ResultFormatterConfig, frames: Dict[str, pd.DataFrame]) -> list:
        if not config.formats:
            return self._map_default(frames)
        else:
            data = []
            max_count = 0
            for f in frames:
                current_length = len(frames[f].index)
                if current_length > max_count:
                    max_count = current_length
            for key in config.formats:
                d = self._map_segment(config.formats[key], frames)
                if len(d.index) == 1:
                    # Multiple Content
                    data.append(pd.DataFrame({
                        key: d.to_dict('records') * max_count
                    }))
                else:
                    data.append(pd.DataFrame({
                        key: d.to_dict('records')
                    }))
            if len(data) == 1:
                return data[0].to_dict('records')
            else:
                return pd.concat(data, axis=1).to_dict('records')

    def _map_default(self, frames: Dict[str, pd.DataFrame]):
        data = []
        max_count = 0
        for f in frames:
            current_length = len(frames[f].index)
            if current_length > max_count:
                max_count = current_length
        for f in frames:
            if len(frames[f].index) == 1:
                # Multiple Content
                data.append(pd.DataFrame({
                    f: frames[f].to_dict('records') * max_count
                }))
            else:
                data.append(pd.DataFrame({
                    f: frames[f].to_dict('records')
                }))
        if len(data) == 1:
            return data[0].to_dict('records')
        else:
            return pd.concat(data, axis=1).to_dict('records')

    def _map_segment(self, segment: List[ResultFieldFormat], frames):
        field_frames = []
        for field in segment:
            if "." in field.value:
                splits = field.value.split(".")
                f = frames[splits[-2]][splits[-1]].to_frame()
                f = f.rename(columns={f.columns[0]: field.name})
                field_frames.append(f)
            else:
                count = len(field_frames[0].index)
                generated_data = getattr(generator, field.value)().run_multiple(count)
                field_frames.append(pd.DataFrame({field.name: generated_data}))
        return pd.concat(field_frames, axis=1)

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
