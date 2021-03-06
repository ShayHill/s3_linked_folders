Manually sync a local folder to an AWS s3 bucket.

# WARNING 1
Filesystem changes are permanent. If you use existing folder names or set up a synced folder with the wrong path, you can end up deleting or overwriting something you didn't plan to. Also, I do not guarantee the safety of your files, even with `safe=True`. This is alpha software. Use at your own risk (and pick unique folder names).

# WARNING 2
The tests create files on your system and your AWS account. Not virtual files, real files. These tests write to your harddrive!

# WARNING 3
I wouldn't try this on Linux. To get subfolders, there's a back and forth between back and forward slashes. I don't have a Linux to test where this falls in that situation.

## purpose?
It's a bad practice to commit binaries to GitHub. If you're not at all interested in VC on your binaries, AWS is a reasonable choice for binary storage, but reassembling your project once code and binaries are in two different places is not straightforward.

Unless I've really missed something, this takes an obscene amount of code. It took too much time to figure this out, so I'm sharing it. Hopefully someone more knowledgeable than I will come along and fork this onto Pypi. Works great for me though.

## install

From your project directory:

`git submodule add https://github.com/ShayHill/s3_linked_folders`

## to use

```
from s3 linked folders import RemoteBucket

link = RemoteBucket(name_of_bucket, absolute_path_to_local_directory)

# push local to AWS, rename conflicts on AWS
link.push()

# push local to AWS, delete conflicts on AWS
link.push(safe=False)

# pull AWS to local, rename conflicts on local
link.pull()

# pull AWS to local, delete conflicts on local
link.pull(safe=False)
```

You'll need a `.git` config folder in your home directory. At a minimum, this folder must contain a file, `config` with the following.

```
[default]
aws_access_key_id = jumbleofcharacters
aws_secret_access_key = jumbleofcharacters
```

## methods comparison

.|`push()`|`push(safe=False)`|`pull()`|`pull(safe=False)`|
--- | --- | --- | --- | --- |
**file on both, names and hatch match**|keep|keep|keep|keep|
**file on both, names match, hatch no**|copy local to remote, rename remote|copy local to remote, delete remote|copy remote to local, rename local|copy remote to local, delete local|
**file on local only**|copy local to remote|copy local to remote|rename local|delete local|
**file on remote only**|rename remote|delete remote|copy remote to local|copy remote to local|
