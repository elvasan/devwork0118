import argparse
import logging

from glue_deployer.deployer_utils import update_or_create_job, destroy_job, \
    update_or_create_all_jobs_for_stage, destroy_all_jobs_for_stage

# Configure Logging
LOGGER = logging.getLogger()
HANDLER = logging.StreamHandler()
FORMATTER = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(HANDLER)
LOGGER.setLevel(logging.INFO)


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
    parser_create_jobs.add_argument('-u', '--user', help='User name used for folder creation under etl-code')
    parser_create_jobs.add_argument('-a', '--all', help='Stage name to create all jobs for.')
    parser_create_jobs.add_argument('-v', '--verbose', action='store_true', help='Turns debug logging on, very chatty.')

    # Destroy Glue Jobs
    parser_destroy_jobs = subparsers.add_parser('job-destroy', help='Destroys an existing AWS Glue Job.')
    parser_create_jobs.set_defaults(which='job-destroy')
    parser_destroy_jobs.add_argument('-e', '--environment', help='Environment name to use in the S3 buckets')
    parser_destroy_jobs.add_argument('-u', '--user', help='User for which you want to delete jobs')
    parser_destroy_jobs.add_argument('-j', '--jobfile', help='Local path to the glue job pyspark file')
    parser_destroy_jobs.add_argument('-a', '--all', help='Stage name to destroy all jobs for.')
    parser_destroy_jobs.add_argument('-v', '--verbose', action='store_true', help='Turn debug logging on, very chatty.')

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    # job subparsers
    if args.subcommand == 'job-create':
        if args.verbose:
            LOGGER.setLevel(logging.DEBUG)
        if not args.user:
            args.user = 'glue'
        if args.all:
            update_or_create_all_jobs_for_stage(args.all,
                                                args.environment,
                                                args.user)
        else:
            update_or_create_job(jobfile=args.jobfile,
                                 env=args.environment,
                                 user=args.user)
    elif args.subcommand == 'job-destroy':
        if args.verbose:
            LOGGER.setLevel(logging.DEBUG)
        if args.all:
            destroy_all_jobs_for_stage(args.all,
                                       args.environment,
                                       args.user)
        else:
            destroy_job(jobfile=args.jobfile,
                        env=args.environment,
                        user=args.user)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
