from transformer import config, result
import transformer.response as rsp
from transformer.result_mapper import JsonArrayResultMapper, DefaultArrayResultMapper
from transformer.library import logger, aws_service

log = logger.set_logger(__name__)

class AbstractExecutor:
    def run(self, **kwargs): pass


class LambdaFixedWidthExecutor(AbstractExecutor):
    def run(self, **kwargs) -> rsp.ResultResponse:
        bucket = kwargs['bucket']
        key = kwargs['key']
        # Initialise Executor Config
        cls = config.ExecutorConfig(key)
        src_mapper = config.SourceMapperConfig(cls)
        # Download Data from S3
        file = aws_service.download_s3_file(
            bucket=bucket,
            key=key,
            file_name="/tmp/tmp_file.txt"
        )
        # Run Source Mapper
        source_dfs = {}
        for m in src_mapper.get_mappers():
            source_dfs[m.get_segment()] = m.run(file)
        # Run Result Mapper
        result_mapper_config = config.ResultMapperConfig(cls)
        if result_mapper_config.get_result_config():
            result_mapper = JsonArrayResultMapper(result_mapper_config)
        else:
            result_mapper = DefaultArrayResultMapper(result_mapper_config)
        result_data = result_mapper.run(source_dfs)
        # Run Result
        result_config = config.ResultConfig(cls)
        response = getattr(result, result_config.get_name())(result_config).run(result_data)
        return rsp.ResultResponse(destination={})


class FixedWidthExecutor(AbstractExecutor):
    pass