import logging
import argparse

from glue_deployer.deployer_utils import update_or_create_job, destroy_job, \
    update_or_create_all_jobs_for_stage, destroy_all_jobs_for_stage

# Configure Logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Parser
def create_parser():
    parser = argparse.ArgumentParser(description='AWS Glue Deployer')
    subparsers = parser.add_subparsers(dest='subcommand', help='sub-command help')
    subparsers.required = True

    # Create Glue Jobs
    parser_create_jobs = subparsers.add_parser('job-create', help='Creates a new AWS Glue Job.')
    parser_create_jobs.set_defaults(which='job-create')
    parser_create_jobs.add_argument('-e', '--environment', help='Environment name to use in the S3 buckets')
    parser_create_jobs.add_argument('-j', '--jobfile', help='Local path to the glue job pyspark file')
    parser_create_jobs.add_argument('-a', '--all', help='Stage name to create all jobs for.')
    parser_create_jobs.add_argument('-v', '--verbose', action='store_true', help='Turns debug logging on, very chatty.')

    # Destroy Glue Jobs
    parser_destroy_jobs = subparsers.add_parser('job-destroy', help='Destroys an existing AWS Glue Job.')
    parser_create_jobs.set_defaults(which='job-destroy')
    parser_destroy_jobs.add_argument('-e', '--environment', help='Environment name to use in the S3 buckets')
    parser_destroy_jobs.add_argument('-j', '--jobfile', help='Local path to the glue job pyspark file')
    parser_destroy_jobs.add_argument('-a', '--all', help='Stage name to destroy all jobs for.')
    parser_destroy_jobs.add_argument('-v', '--verbose', action='store_true', help='Turns debug logging on, very chatty.')

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    # job subparsers
    if args.subcommand == 'job-create':
        if args.verbose:
            logger.setLevel(logging.DEBUG)
        if args.all:
            update_or_create_all_jobs_for_stage(args.all,
                                                args.environment)
        else:
            update_or_create_job(jobfile=args.jobfile,
                                 env=args.environment)
    elif args.subcommand == 'job-destroy':
        if args.verbose:
            logger.setLevel(logging.DEBUG)
        if args.all:
            destroy_all_jobs_for_stage(args.all,
                                       args.environment)
        else:
            pass
            destroy_job(jobfile=args.jobfile,
                        env=args.environment)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
