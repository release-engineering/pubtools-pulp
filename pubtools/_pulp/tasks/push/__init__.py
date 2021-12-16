from .command import Push


def entry_point(cls=Push):
    cls().main()


def doc_parser():
    return Push().parser
