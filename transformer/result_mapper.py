from transformer.config import ResultMapperConfig
import sys
import pandas as pd
import transformer.generator as generator

current_module = sys.modules[__name__]


class AbstractResultMapper:
    config = dict

    def __init__(self, config: ResultMapperConfig):
        self.config = config.get_result_config()

    def run(self, input_data: dict[pd.DataFrame]): pass


class SourceArrayResultMapper(AbstractResultMapper):

    def __init__(self, config: ResultMapperConfig):
        super().__init__(config)

    def run(self, input_data: dict[pd.DataFrame]):
        maximum_index = ""
        maximum_count = 0
        for df in input_data:
            input_data[df] = input_data[df].to_dict('records')
            if len(input_data[df]) > maximum_count:
                maximum_index = df
                maximum_count = len(input_data[df])
        result_data = []
        for itr in range(len(input_data[maximum_index])):
            record = {}
            for df in input_data:
                record[df] = input_data[df]
            result_data.append(record)
        return result_data


class JsonArrayResultMapper(AbstractResultMapper):
    """
    This ResultMapper maps incoming dataframes and returns it as list
    """
    def __init__(self, config: ResultMapperConfig):
        super().__init__(config)

    def run(self, input_data: dict[pd.DataFrame]):
        return self.__list__(self.recursion(input_data, self.config))

    def __list__(self, dfs):
        print(dfs)
        max_count = 1
        if isinstance(dfs, list):
            return dfs
        for df in dfs:
            count = len(dfs[df])
            if isinstance(dfs[df], dict):
                continue
            else:
                if count > max_count:
                    max_count = count

        for df in dfs:
            if len(dfs[df]) < max_count:
                dfs[df] = dfs[df] * max_count

        final_result = []
        for itr in range(max_count):
            data = {}
            for df in dfs:
                data[df] = dfs[df][itr]
            final_result.append(data)
        return final_result

    def recursion(self, input_data, node):
        if isinstance(node, list):
            print("Deepest depth reached! Data: {}".format(node))
            columns = []
            to_generate = []
            has_reference_field = False
            for n in node:
                if 'input' in n['input']:
                    has_reference_field = True
            if has_reference_field:
                target_segment = ""
                for n in node:
                    if 'input' in n['input']:
                        splits = n['input'].replace("{", "").replace("}", "").split('.')
                        target_segment = splits[-2]
                        columns.append(splits[-1])
                    else:
                        to_generate.append(n)
                column_count = len(input_data[target_segment].index)
                selected = input_data[target_segment][columns]
                to_append = {}
                for g in to_generate:
                    to_append[g['name']] = getattr(generator, g['input'])().run_multiple(column_count)
                to_append_df = pd.DataFrame(to_append)
                concatenated = pd.concat([selected, to_append_df], axis=1)
                return concatenated.to_dict('records')
            else:
                # This segment is for data that has no reference, meaning its a straight up generation type.
                to_append = {}
                for n in node:
                    to_append[n['name']] = getattr(generator, n['input'])().run()
                return [to_append]
        else:
            if len(node.keys()) == 1:
                for depth in node:
                    return {depth: self.recursion(input_data, node[depth])}
            else:
                results = {}
                for depth in node:
                    results[depth] = self.recursion(input_data, node[depth])
                return results


class JsonResultMapper(AbstractResultMapper):
    def __init__(self, config: ResultMapperConfig):
        super().__init__(config)

    def run(self, input_data: dict[pd.DataFrame]):
        return self.recursion(input_data, self.config)

    def recursion(self, input_data, node):
        if isinstance(node, list):
            print("Deepest depth reached! Data: {}".format(node))
            columns = []
            to_generate = []
            has_reference_field = False
            for n in node:
                if 'input' in n['input']:
                    has_reference_field = True
            if has_reference_field:
                target_segment = ""
                for n in node:
                    if 'input' in n['input']:
                        splits = n['input'].replace("{", "").replace("}", "").split('.')
                        target_segment = splits[-2]
                        columns.append(splits[-1])
                    else:
                        to_generate.append(n)
                column_count = len(input_data[target_segment].index)
                selected = input_data[target_segment][columns]
                to_append = {}
                for g in to_generate:
                    to_append[g['name']] = getattr(generator, g['input'])().run_multiple(column_count)
                to_append_df = pd.DataFrame(to_append)
                concatenated = pd.concat([selected, to_append_df], axis=1)
                return concatenated.to_dict('records')
            else:
                # This segment is for data that has no reference, meaning its a straight up generation type.
                to_append = {}
                for n in node:
                    to_append[n['name']] = getattr(generator, n['input'])().run()
                return to_append
        else:
            print(node)
            if len(node.keys()) == 1:
                for depth in node:
                    return {depth: self.recursion(input_data, node[depth])}
            else:
                results = {}
                for depth in node:
                    results[depth] = self.recursion(input_data, node[depth])
                return results
