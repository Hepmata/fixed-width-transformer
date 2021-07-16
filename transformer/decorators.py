class PreValidate(object):
    def __init__(self, function):
        self.function = function

    def __call__(self, *args, **kwargs):
        print(args)
        self.function(self, args[1])
        # Run Validation


class PostValidate(object):
    def __init__(self, function):
        self.function = function

    def __call__(self, *args, **kwargs):
        self.function()