package Catmandu::Store::DBI;

use Catmandu::Sane;
use Catmandu::Util qw(require_package);
use DBI;
use Catmandu::Store::DBI::Bag;
use Moo;
use namespace::clean;

our $VERSION = "0.0502";

with 'Catmandu::Store';

has data_source => (
    is => 'ro',
    required => 1,
    trigger => sub { $_[0] =~ /^DBI:/i ? $_[0] : "DBI:$_[0]" },
);
has username => (is => 'ro', default => sub { '' });
has password => (is => 'ro', default => sub { '' });
has timeout => (is => 'ro', predicate => 1);
has reconnect_after_timeout => (is => 'ro');
has handler => (is => 'lazy');
has _in_transaction => (
    is => 'rw',
    writer => '_set_in_transaction',
);
has _connect_time => (is => 'rw', writer => '_set_connect_time');
has _dbh => (
    is => 'lazy',
    builder => '_build_dbh',
    writer => '_set_dbh',
);

sub handler_namespace {
   'Catmandu::Store::DBI::Handler';
}

sub _build_handler {
    my ($self) = @_;
    my $driver = $self->dbh->{Driver}{Name} // '';
    my $ns = $self->handler_namespace;
    my $pkg;
    if ($driver =~ /pg/i) {
        $pkg = 'Pg';
    } elsif ($driver =~ /sqlite/i) {
        $pkg = 'SQLite';
    } elsif ($driver =~ /mysql/i) {
        $pkg = 'MySQL';
    } else {
        Catmandu::NotImplemented->throw(
            'Only Pg, SQLite and MySQL are supported.');
    }
    require_package($pkg, $ns)->new;
}

sub _build_dbh {
    my ($self) = @_;
    my $opts = {
        AutoCommit => 1,
        RaiseError => 1,
        mysql_auto_reconnect => 1,
    };
    my $dbh = DBI->connect(
        $self->data_source,
        $self->username,
        $self->password,
        $opts,
    );
    $self->_set_connect_time(time);
    $dbh;
}

sub dbh {
    my ($self) = @_;
    my $dbh = $self->_dbh;
    my $connect_time = $self->_connect_time;
    my $driver = $dbh->{Driver}{Name} // '';

    # MySQL has builtin option mysql_auto_reconnect
    if ($driver !~ /mysql/i && $self->has_timeout &&
            time - $connect_time > $self->timeout) {
        if ($self->reconnect_after_timeout || !$dbh->ping) {
            # ping failed, so try to reconnect
            $dbh->disconnect;
            $dbh = $self->_build_dbh;
            $self->_set_dbh($dbh);
        } else {
            $self->_set_connect_time(time);
        }
    }

    $dbh;
}

sub transaction {
    my ($self, $sub) = @_;

    if ($self->_in_transaction) {
        return $sub->();
    }

    my $dbh = $self->dbh;
    my @res;

    eval {
        $self->_set_in_transaction(1);
        $dbh->begin_work;
        @res = $sub->();
        $dbh->commit;
        $self->_set_in_transaction(0);
        1;
    } or do {
        my $err = $@;
        eval { $dbh->rollback };
        $self->_set_in_transaction(0);
        die $err;
    };

    @res;
}

sub DEMOLISH {
    my ($self) = @_;
    $self->{_dbh}->disconnect if $self->{_dbh};
}

1;

=head1 NAME

Catmandu::Store::DBI - A Catmandu::Store plugin for DBI based interfaces

=head1 VERSION

Version 0.0424

=head1 SYNOPSIS

    use Catmandu::Store::DBI;

    my $store = Catmandu::Store::DBI->new(
        data_source => 'DBI:mysql:database=test', # prefix "DBI:" optional
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

The L<catmandu> command line client can be used like this:

    catmandu import JSON to DBI --data_source SQLite:mydb.sqlite < data.json

=head1 DESCRIPTION

A Catmandu::Store::DBI is a Perl package that can store data into
DBI backed databases. The database as a whole is called a 'store'
(L<Catmandu::Store>. Databases also have compartments (e.g. tables)
called 'bags' (L<Catmandu::Bag>).

=head1 COLUMN MAPPING

The default behavior is to map the C<_id> of the record to the C<id> column and serialize all other data in the C<data> column. This behavior can be changed with mapping option:

    my $store = Catmandu::Store::DBI->new(
        data_source => 'DBI:mysql:database=test',
        bags => {
            # books table
            books => {
                mapping => {
                    # these keys will be directly mapped to columns
                    # all other keys will be serialized in the data column
                    title => {type => 'string', required => 1, column => 'book_title'},
                    isbn => {type => 'string', unique => 1},
                    authors => {type => 'string', array => 1}
                }
            }
        }
    );

=head2 Column types

=over

=item string

=item integer

=item binary

=back

=head2 Column options

=over

=item column

Name of the table column if it differs from the key in your data.

=item array

Boolean option, default is C<0>. Note that this options is only supported for PostgreSQL.

=item unique

Boolean option, default is C<0>.

=item required

Boolean option, default is C<0>.

=back

=head1 METHODS

=head2 new(data_source => $data_source)

Create a new Catmandu::Store::DBI store using a DBI $data_source. The
prefix "DBI:" is added automatically if needed.

Extra options for method new:

timeout

        timeout for a inactive database handle.
        when timeout is reached, Catmandu checks if the connection is still alive (by use of ping),
        or it recreates the connection.

        By default set to undef.

reconnect_after_timeout

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


=head2 bag($name)

Create or retieve a bag with name $name. Returns a Catmandu::Bag.

=head1 SEE ALSO

L<Catmandu::Bag>, L<Catmandu::Searchable>, L<DBI>

=cut
