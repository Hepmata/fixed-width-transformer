import datetime
import os
import uuid

from transformer.library import logger, aws_service
from transformer.executor import ExecutorConfig
from transformer.source import SourceMapperConfig, SourceMapper
from transformer.result import ResultMapperConfig, ResultProducerConfig
from transformer.result import ResultMapper, result_producer
from transformer.constraints import constraints
from transformer.model import response_model
from pytz import timezone
from transformer.library import exceptions

log = logger.set_logger(__name__)


class AbstractExecutor:
    def run(self, **kwargs):
        pass


class LambdaFixedWidthExecutor(AbstractExecutor):
    def __init__(self):
        self.file_name = ''
        self.source_mapper_config = None

    def run_constraints(self):
        log.info('Running Constraint checks...')
        for constraint in self.source_mapper_config.get_constraints():
            log.info(f'Running {constraint.name} check...')
            constraint.arguments['source_file_path'] = self.file_name
            getattr(constraints, constraint.name)().run(
                self.source_mapper_config,
                constraint.arguments
            )
            log.info(f'{constraint.name} check passed!')

    def run(self, **kwargs) -> response_model.ExecutorResponse:
        bucket = kwargs['bucket']
        key = kwargs['key']
        tz = timezone('Asia/Singapore')
        try:
            # 1. Download Config/Locate Config & Initialise Config
            # Compulsory Segment
            cls = ExecutorConfig(key)

            # 2. Download Source Data/File
            # Compulsory segment
            if 'mount_path' in os.environ.keys():
                self.file_name = f"{os.environ['mount_path']}/{key.replace('/', '_')}"
            else:
                self.file_name = f"/tmp/{key.replace('/', '_')}"
            aws_service.download_s3_file(bucket=bucket, key=key, file_name=self.file_name)

            # Compulsory Segment
            self.source_mapper_config = SourceMapperConfig(
                config=cls.get_exact_config(), file_name=self.file_name
            )
            # 2.1: Run Constraints
            self.run_constraints()
            # 3. Run SourceMapper
            dataframes = SourceMapper().run(self.source_mapper_config)
            # 4. Run ResultMapper
            result_mapper_config = ResultMapperConfig(cls.get_exact_config())
            result_config = ResultProducerConfig(cls.get_exact_config())
            # Conditional Segment
            is_empty = False
            empty_segment = ''
            highest_record_count = 0
            for df in dataframes:
                if dataframes[df].empty:
                    is_empty = True
                    empty_segment = df
                if len(dataframes[df].index) > highest_record_count:
                    highest_record_count = len(dataframes[df].index)
            if is_empty:
                log.info(
                    f'Dataframe segment {empty_segment} is empty, skipping result generation.'
                )
                highest_record_count = 0
            else:
                result_mapper = ResultMapper()
                result_data = result_mapper.run(
                    config=result_mapper_config, frames=dataframes
                )
                # 5. Run ResultProducer
                # # Conditional Segment
                for producer in result_config.producers:
                    getattr(result_producer, producer.name)(producer).run(result_data)
            # 6. Return Result
            validators = (
                self.source_mapper_config.get_validation_array()
                + result_mapper_config.get_validation_array()
            )
            topics = []
            for producer in result_config.producers:
                if 'topic' in producer.arguments.keys():
                    topics.append(producer.arguments['topic'])

            return response_model.SuccessResultResponse(
                sourceBucket=bucket,
                sourceFile=key,
                requestTime=datetime.datetime.now(tz).isoformat(),
                requestId=kwargs['requestId'],
                recordCount=highest_record_count,
                validations=validators,
                constraints=self.source_mapper_config.get_constraint_array(),
                resultTopic=','.join(topics),
                statusCode=200,
            )
        except exceptions.ValidationFailureError as e:
            validator_output = []
            for error in e.errors:
                validator_output.append(
                    {
                        'segment': error.segment,
                        'field': error.fieldName,
                        'validator': error.validatorName,
                        'failureCount': error.failCount,
                        'recordCount': error.recordCount,
                        'message': str(error),
                    }
                )
            return response_model.ValidationErrorResponse(
                sourceBucket=bucket,
                sourceFile=key,
                requestTime=datetime.datetime.now(tz).isoformat(),
                requestId=kwargs['requestId'],
                recordCount=e.errors[0].recordCount,
                validations=validator_output,
                failureReason=type(e).__name__,
                statusCode=400,
            )
        except exceptions.ConfigError as e:
            return response_model.GenericErrorResponse(
                sourceBucket=bucket,
                sourceFile=key,
                requestTime=datetime.datetime.now(tz).isoformat(),
                requestId=kwargs['requestId'],
                failureReason=type(e).__name__,
                failureMessage=str(e),
                statusCode=500,
            )
        except exceptions.FailedConstraintsException as e:
            return response_model.ConstraintErrorResponse(
                sourceBucket=bucket,
                sourceFile=key,
                requestTime=datetime.datetime.now(tz).isoformat(),
                requestId=kwargs['requestId'],
                failureReason=type(e).__name__,
                constraints=self.source_mapper_config.get_constraint_array(),
                statusCode=500,
                recordCount=0
            )
        except exceptions.FailedConstraintException as e:
            return response_model.ConstraintErrorResponse(
                sourceBucket=bucket,
                sourceFile=key,
                requestTime=datetime.datetime.now(tz).isoformat(),
                requestId=kwargs['requestId'],
                failureReason=type(e).__name__,
                failureMessage=str(e),
                constraints=self.source_mapper_config.get_constraint_array(),
                statusCode=500,
                recordCount=e.recordCount
            )
        except Exception as e:
            log.exception(e)
            return response_model.GenericErrorResponse(
                sourceBucket=bucket,
                sourceFile=key,
                requestTime=datetime.datetime.now(tz).isoformat(),
                requestId=kwargs['requestId'],
                failureReason=type(e).__name__,
                failureMessage=str(e),
                statusCode=500,
            )
