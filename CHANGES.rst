Changelog
=========

3.0.0 (unreleased)
------------------

- Migrated to Plone 6.2 and Python 3, dropped Plone 4, based on the work started by @bsuttor on `plone6`.
  [bsuttor, chris-adam]
- Replaced ftw.labels by collective.labels, dropped the documentviewer and cron4plone settings changes (gone from imio.dms.mail 4).
  [chris-adam]
- Pipelines: `datesupdater` runs again after `reindexobject`, which sets the modification date to now since Plone 5.1 (to report in the customer pipelines).
  [chris-adam]
- Added tests and `collective.behavior.internalnumber`, `collective.transmogrifier`, `plone.base` to install_requires.
  [chris-adam]

2.0 (unreleased)
------------------

- Added content_category value on files
  [sgeulette]
- Added 2_missing_files.csv output files to list missing files
  [sgeulette]
- Added _company info in contact, _ref_recom in mails
  [sgeulette]
- Part l: got mail_type from nature and send_modes from source, like part p.
  [sgeulette]
- Stored contacts and other texts in description when the data_transfer behavior is not used.
  [sgeulette]
- Handling IMIO customer types 1 on dmsmail 3.1.x.
  [sgeulette]

1.0 (unreleased)
------------------

- Handling IMIO customer types 1, 1b, 1c, 2, 3, 4 on dmsmail 3.0.x.
  [sgeulette]
- Initial release.
  [sgeulette]
