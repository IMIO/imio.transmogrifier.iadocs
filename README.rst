.. This README is meant for consumption by humans and pypi. Pypi can render rst files so please do not use Sphinx features.
   If you want to learn more about writing documentation, please check out: http://docs.plone.org/about/documentation_styleguide.html
   This text does not appear on pypi or github. It is a comment.

==========================
imio.transmogrifier.iadocs
==========================

Transmogrifier data import to ia.docs

Features
========

- executes a pipeline with a run script
- provides specific ia.docs blueprints

    - data-transfer.cfg.1 : I8s (id on B column)
    - data-transfer.cfg.1b : I8s (id on C column)
    - data-transfer.cfg.2 : A8e
    - data-transfer.cfg.3 : B7c
    - data-transfer.cfg.4 : E8o
    - data-transfer.cfg.5 : I8s (new unknown format)

- production pipelines:

    - data-transfer.cfg.1.ais : I8s (.1 but no addresses view)

Installation
============

Install imio.transmogrifier.iadocs by adding it to your buildout::

    [buildout]

    ...

    eggs =
        imio.transmogrifier.iadocs


and then running ``bin/buildout``

Contribute
==========

- Issue Tracker: https://github.com/imio/imio.transmogrifier.iadocs/issues
- Source Code: https://github.com/imio/imio.transmogrifier.iadocs

License
=======

The project is licensed under the GPLv2.
