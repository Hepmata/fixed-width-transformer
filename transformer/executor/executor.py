
from transformer.library import logger, aws_service
from transformer.executor import ExecutorConfig
from transformer.source import SourceMapperConfig, SourceMapper
from transformer.source import source_mapper
from transformer.result import ResultMapperConfig, ResultProducerConfig
from transformer.result import ResultMapper, result_producer
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
        file_name = "/tmp/" + key.replace("/", "_")
        file = aws_service.download_s3_file(
            bucket=bucket,
            key=key,
            file_name=file_name
        )
        # 3. Run SourceMapper
        # Compulsory Segment
        src_mapper_cfg = SourceMapperConfig(config=cls.get_exact_config(), file_name=file_name)
        dataframes = SourceMapper().run(src_mapper_cfg)
        # 4. Run ResultMapper
        # Conditional Segment
        result_mapper_config = ResultMapperConfig(cls.get_exact_config())
        result_mapper = ResultMapper()
        result_data = result_mapper.run(config=result_mapper_config,frames=dataframes)
        # 5. Run ResultProducer
        # # Conditional Segment
        result_config = ResultProducerConfig(cls.get_exact_config())
        response = getattr(result_producer, result_config.name)(result_config).run(result_data)

        # 6. Return Result
        return ResultResponse(destination={})


# class FixedWidthExecutor(AbstractExecutor):
#     pass