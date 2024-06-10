from .command import Push


def entry_point(cls=Push):
    with cls() as instance:
        instance.main()


def doc_parser():
    return Push().parser
