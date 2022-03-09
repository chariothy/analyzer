import fire

def msrank():
    from morningstar.msrank import report_top_rank
    report_top_rank()

if __name__ == "__main__":
    fire.Fire()