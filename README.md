# NAME

Catmandu::Store::DBI - A Catmandu::Store plugin for DBI based interfaces

# VERSION

Version 0.0424

# SYNOPSIS

    use Catmandu::Store::DBI;

    my $store = Catmandu::Store::DBI->new(
        data_source => 'DBI:mysql:database=test', # prefix "DBI:" optionl
        username => '', # optional
        password => '', # optional
    );

    my $obj1 = $store->bag->add({ name => 'Patrick' });

    printf "obj1 stored as %s\n" , $obj1->{_id};

    # Force an id in the store
    my $obj2 = $store->bag->add({ _id => 'test123' , name => 'Nicolas' });

    my $obj3 = $store->bag->get('test123');

    $store->bag->delete('test123');

    $store->bag->delete_all;

    # All bags are iterators
    $store->bag->each(sub { ... });
    $store->bag->take(10)->each(sub { ... });

The [catmandu](https://metacpan.org/pod/catmandu) command line client can be used like this:

    catmandu import JSON to DBI --data_source SQLite:mydb.sqlite < data.json

# DESCRIPTION

A Catmandu::Store::DBI is a Perl package that can store data into
DBI backed databases. The database as a whole is called a 'store'
([Catmandu::Store](https://metacpan.org/pod/Catmandu::Store). Databases also have compartments (e.g. tables)
called 'bags' ([Catmandu::Bag](https://metacpan.org/pod/Catmandu::Bag)).

# METHODS

## new(data\_source => $data\_source)

Create a new Catmandu::Store::DBI store using a DBI $data\_source. The
prefix "DBI:" is added automatically if needed.

Extra options for method new:

timeout

        timeout for a inactive database handle.
        when timeout is reached, Catmandu checks if the connection is still alive (by use of ping),
        or it recreates the connection.

        By default set to undef.

reconnect\_after\_timeout

        when timeout is reached, Catmandu does not check the connection, but simply reconnects.

        By default set to '0'

It's good practice to set the timeout high enough.
When using transactions, one should avoid this situation:

    $bag->store->transaction(sub{
        $bag->add({ _id => "1" });
        sleep $timeout;
        $bag->add({ _id => "2" });
    });

The following warning appears:

    commit ineffective with AutoCommit enabled at lib//Catmandu/Store/DBI.pm line 73.
    DBD::SQLite::db commit failed: attempt to commit on inactive database handle

This has the following reasons:

    1.  first record added
    2.  timeout is reached, the connection is recreated
    3.  the option AutoCommit is set. So the database handle commits the current transaction. The first record is committed.
    4.  this new connection handle is used now. We're still in the method "transaction", but there is no longer a real transaction at database level.
    5.  second record is added (committed)
    6.  commit is issued. But this unnecessary, so the database handle throws a warning.

## bag($name)

Create or retieve a bag with name $name. Returns a Catmandu::Bag.

# AUTHOR

Nicolas Steenlant, `<nicolas.steenlant at ugent.be>`

# CONTRIBUTOR

Vitali Peil `<vitali.peil at uni-bielefeld.de>`

# CONTRIBUTOR

Nicolas Franck `<nicolas.franck at ugent.be>`

# SEE ALSO

[Catmandu::Bag](https://metacpan.org/pod/Catmandu::Bag), [Catmandu::Searchable](https://metacpan.org/pod/Catmandu::Searchable), [DBI](https://metacpan.org/pod/DBI)
