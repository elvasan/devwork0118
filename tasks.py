from invoke import task


@task
def updateglue(ctx):
    ctx.run("rm -rf .awsglue/awsglue")
    ctx.run("curl https://codeload.github.com/awslabs/aws-glue-libs/tar.gz/master | \
             tar -xvz --strip=1 aws-glue-libs-master/awsglue")
    ctx.run("mv awsglue .awsglue")
    ctx.run("cd .awsglue && pip install . --upgrade")


@task
def combinecsvs(ctx, headersrc, csvdir, csvdest):
    ctx.run("head -1 {} > {}".format(headersrc, csvdest))
    ctx.run("tail -n +2 {}/*.csv >> {}".format(csvdir, csvdest))
