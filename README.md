# NAME

Catmandu::DBI - Catmandu tools to communicate with DBI based interfaces

# STATUS

[![Build Status](https://travis-ci.org/LibreCat/Catmandu-DBI.svg?branch=master)](https://travis-ci.org/LibreCat/Catmandu-DBI)
[![Coverage](https://coveralls.io/repos/LibreCat/Catmandu-DBI/badge.png?branch=master)](https://coveralls.io/r/LibreCat/Catmandu-DBI)
[![CPANTS kwalitee](http://cpants.cpanauthors.org/dist/Catmandu-DBI.png)](http://cpants.cpanauthors.org/dist/Catmandu-DBI)

# SYNOPSIS

    # From the command line

    # Export data from a relational database
    $ catmandu convert DBI --dsn dbi:mysql:foobar --user foo --password bar --query "select * from table"

    # Import data into a relational database
    $ catmandu import JSON to DBI --data_source dbi:SQLite:mydb.sqlite < data.json

    # Export data from a relational database
    $ catmandu export DBI --data_source dbi:SQLite:mydb.sqlite to JSON

# MODULES

[Catmandu::Importer::DBI](https://metacpan.org/pod/Catmandu::Importer::DBI)

[Catmandu::Store::DBI](https://metacpan.org/pod/Catmandu::Store::DBI)

# AUTHORS

Nicolas Franck `<nicolas.franck at ugent.be>`

Patrick Hochstenbach `<patrick.hochstenbach at ugent.be>`

Vitali Peil `<vitali.peil at uni-bielefeld.de>`

Nicolas Steenlant `<nicolas.steenlant at ugent.be>`

# SEE ALSO

[Catmandu](https://metacpan.org/pod/Catmandu), [Catmandu::Importer](https://metacpan.org/pod/Catmandu::Importer) , [Catmandu::Store::DBI](https://metacpan.org/pod/Catmandu::Store::DBI)
