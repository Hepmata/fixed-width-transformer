import uuid


class AbstractGenerator:
    def run(self, *args, **kwargs):
        pass

    def run_multiple(self, count):
        pass


class UuidGenerator(AbstractGenerator):
    def run(self, *args, **kwargs):
        return uuid.uuid4().__str__()

    def run_multiple(self, count):
        uuids = []
        [uuids.append(uuid.uuid4().__str__()) for _ in range(count)]
        return uuids


class IdGenerator(AbstractGenerator):
    def run(self, *args, **kwargs):
        return 1

    def run_multiple(self, count):
        data = []
        [data.append(itr + 1) for itr in range(count)]
        return data
