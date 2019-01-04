============================================
Add New Backend Storage To Zenko CloudServer
============================================

This set of documents aims at bootstrapping developers with Zenko's CloudServer
module, so they can then go on and contribute features.

.. toctree::
    :maxdepth: 2

    non-s3-compatible-backend
    s3-compatible-backend

We always encourage our community to offer new extensions to Zenko,
and new backend support is paramount to meeting more community needs.
If that is something you want to contribute (or just do on your own
version of the cloudserver image), this is the guid to read. Please
make sure you follow our `Contributing Guidelines`_/.

If you need help with anything, please search our `forum`_ for more
information.

Add support for a new backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Currently the main public cloud protocols are supported and more can
be added. There are two main types of backend: those compatible with
Amazon's S3 protocol and those not compatible.

================= ========== ============ ===========
Backend type      Supported  Active WIP   Not started
================= ========== ============ ===========
Private disk/fs       x
AWS S3                x
Microsoft Azure       x
Backblaze B2                    x
Google Cloud          x
Openstack Swift                               x
================= ========== ============ ===========

.. important:: Should you want to request for a new backend to be
               supported, please do so by opening a `Github issue`_,
               and filling out the "Feature Request" section of our
               template.

To add support for a new backend support to CloudServer official
repository, please follow these steps:

- familiarize yourself with our `Contributing Guidelines`_
- open a `Github issue`_ and fill out Feature Request form, and
  specify you would like to contribute it yourself;
- wait for our core team to get back to you with an answer on whether
  we are interested in taking that contribution in (and hence
  committing to maintaining it over time);
- once approved, fork the repository and start your development;
- use the `forum`_ with any question you may have during the
  development process;
- when you think it's ready, let us know so that we create a feature
  branch against which we'll compare and review your code;
- open a pull request with your changes against that dedicated feature
  branch;
- once that pull request gets merged, you're done.

.. tip::

    While we do take care of the final rebase (when we merge your feature
    branch on the latest default branch), we do ask that you keep up to date with our latest default branch
    until then.

.. important::

    If we do not approve your feature request, you may of course still
    work on supporting a new backend: all our "no" means is that we do not
    have the resources, as part of our core development team, to maintain
    this feature for the moment.

.. _GitHub issue: https://github.com/scality/S3/issues
.. _Contributing Guidelines: https://github.com/scality/Guidelines/blob/master/CONTRIBUTING.md
.. _forum: https://forum.zenko.io
