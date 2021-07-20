
from transformer.library import logger, aws_service

log = logger.set_logger(__name__)

# 
# class AbstractExecutor:
#     def run(self, **kwargs): pass
# 
# 
# class LambdaFixedWidthExecutor(AbstractExecutor):
#     def run(self, **kwargs) -> rsp.ResultResponse:
#         bucket = kwargs['bucket']
#         key = kwargs['key']
#         # 1. Download Config/Locate Config & Initialise Config
#         # Compulsory Segment
#         cls = config.ExecutorConfig(key)
#         # 2. Download Source Data/File
#         # Compulsory segment
#         file = aws_service.download_s3_file(
#             bucket=bucket,
#             key=key,
#             file_name="/tmp/tmp_file.txt"
#         )
#         # 3. Run SourceMapper
#         # Compulsory Segment
#         src_mapper = config.SourceMapperConfig(cls)source_dfs = {}
#         for m in src_mapper.get_mappers():
#             source_dfs[m.get_segment()] = m.run(file)
#         # 4. Run ResultMapper
#         # Conditional Segment
#         result_mapper_config = config.ResultMapperConfig(cls)
#         if result_mapper_config.get_result_config():
#             result_mapper = JsonArrayResultMapper(result_mapper_config)
#         else:
#             result_mapper = DefaultArrayResultMapper(result_mapper_config)
#         result_data = result_mapper.run(source_dfs)
#         # 5. Run ResultProducer
#         # Conditional Segment
#         result_config = config.ResultConfig(cls)
#         response = getattr(result, result_config.get_name())(result_config).run(result_data)
#         
#         # 6. Return Result
#         return rsp.ResultResponse(destination={})
# 
# 
# class FixedWidthExecutor(AbstractExecutor):
#     pass