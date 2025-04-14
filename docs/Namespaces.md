# Namespaces

Namespaces are the fundamental basis for a query.

## Architecture

Namespaces in S3 object storage are simply the full file's name.

In fugu we store all of the necessary pre-processed indexing of a namespace at
that namespace's root, ie `/path/to/a/name/space/.fugu`.

<img src="docs/images/Namespace-Paths.png" alt="drawing" width="200"/>


Now each namespace can also have a child namespace, which we will call a subspace. 

Instead of manage the subspace's index directly from the parent context, we spin
up a new work-stealing child process that idles with a lock on the SSD cached
version of the index.

## Indexing

Since we want to be able to quickly access the most up-to-date information in
the database, all actions by the Node are FINAL. This means that the **only**
program with a write lock to the file is that namespace Node. 

Because of this, we can use.


