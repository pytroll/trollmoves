Remove_it
=========

Remove_it is a script that is made to clean directories, and optionally publish messages about the removed files.

An example config would look like::

        [my_cleaning_job]
        base_dir=/some/path/to/clean
        templates=*
        stat_time_method=st_mtime
        recursive=true
        include_hidden=false

Even if "include_hidden" is set to "true", ".keep" files will never be removed (useful to avoid directories from being
cleaned up)
