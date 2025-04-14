# Security

## Attack Surface and Possible Threats
The attack surface of fugu is quite specific.

The main thing we want to protect are that a Node process is not allowed to:
- Access any files that are not its docs or index (file traversal) on the server
- access any files that are not in its namespace when accessing the S3 bucket (including subspaces)
- enumerate the S3 bucket's files/namespaces
- poison the cache values of other node's actions
- decrypt any any files it knows of

