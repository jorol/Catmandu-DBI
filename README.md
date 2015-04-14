# NAME

Catmandu::DBI - Catmandu tools to communicate with DBI based interfaces

# MODULES

[Catmandu::Importer::DBI](https://metacpan.org/pod/Catmandu::Importer::DBI)

[Catmandu::Store::DBI](https://metacpan.org/pod/Catmandu::Store::DBI)

# BUGS

- versions below 0.0135 contain a bug for mysql: identifier 'id' is of type 'varchar', which is case insensitive. Fixed in version 0.0135. This can easily be solved with this sql statement:
```
MariaDB [imaging]> alter table scans modify id varchar(255) binary;
```

# AUTHORS

Nicolas Franck `<nicolas.franck at ugent.be>`

Patrick Hochstenbach `<patrick.hochstenbach at ugent.be>`

Nicolas Steenlant `<nicolas.steenlant at ugent.be>`

Vitali Peil `<vitali.peil at uni-bielefeld.de>`

# SEE ALSO

[Catmandu](https://metacpan.org/pod/Catmandu), [Catmandu::Importer](https://metacpan.org/pod/Catmandu::Importer),
[Catmandu::Store::DBI](https://metacpan.org/pod/Catmandu::Store::DBI)
