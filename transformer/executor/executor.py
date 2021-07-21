
from transformer.library import logger, aws_service
from transformer.executor import ExecutorConfig
from transformer.source import SourceMapperConfig
from transformer.source import source_mapper
from transformer.result import ResultMapperConfig
from transformer.result import ResultMapper
from transformer.model import ResultResponse
log = logger.set_logger(__name__)


class AbstractExecutor:
    def run(self, **kwargs): pass


class LambdaFixedWidthExecutor(AbstractExecutor):
    def run(self, **kwargs) -> ResultResponse:
        bucket = kwargs['bucket']
        key = kwargs['key']
        # 1. Download Config/Locate Config & Initialise Config
        # Compulsory Segment
        cls = ExecutorConfig(key)
        # 2. Download Source Data/File
        # Compulsory segment
        file = aws_service.download_s3_file(
            bucket=bucket,
            key=key,
            file_name="/tmp/tmp_file.txt"
        )
        # 3. Run SourceMapper
        # Compulsory Segment
        src_mapper = SourceMapperConfig(config=cls)
        dataframes = {}
        for mapping in src_mapper.get_mappers():
            dataframes[mapping.segment] = getattr(source_mapper, mapping.name)().run(mapping, file)
        # 4. Run ResultMapper
        # Conditional Segment
        result_mapper_config = ResultMapperConfig(cls)
        result_mapper = ResultMapper(result_mapper_config)
        result_data = result_mapper.run(dataframes)
        # 5. Run ResultProducer
        # # Conditional Segment
        # result_config = ResultConfig(cls)
        # response = getattr(result, result_config.get_name())(result_config).run(result_data)

        # 6. Return Result
        return ResultResponse(destination={})

# 
# class FixedWidthExecutor(AbstractExecutor):
#     pass