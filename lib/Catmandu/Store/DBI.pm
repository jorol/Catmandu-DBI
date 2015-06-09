package Catmandu::Store::DBI;

use Catmandu::Sane;
use DBI;
use Catmandu::Store::DBI::Bag;
use Moo;
use namespace::clean;

our $VERSION = "0.0425";

with 'Catmandu::Store';

has data_source => (
    is       => 'ro',
    required => 1,
    trigger  => sub { $_[0] =~ /^DBI:/i ? $_[0] : "DBI:$_[0]" },
);

has username => ( is => 'ro', default => sub { '' } );
has password => ( is => 'ro', default => sub { '' } );
has timeout => ( is => 'ro' );
has reconnect_after_timeout => (is => 'ro', default => sub { 0; });

sub dbh {
    my($self,$no_reconnect) = @_;

    state $instances = {};

    unless($instances->{$self}){
        $instances->{$self} = {
            start_time => time,
            dbh => $self->_build_dbh()
        };
    }

    my $dbh = $instances->{$self}->{dbh};

    #do NOT access driver attributes during global destruction!
    return $dbh if $no_reconnect;

    my $driver = $dbh->{Driver}{Name} // "";
    my $start_time = $instances->{$self}->{start_time};

    #mysql has built-in option 'mysql_auto_reconnect'
    if ($driver !~ /mysql/i && defined($self->timeout)) {

        if((time - $start_time) > $self->timeout ) {

            #timeout $timeout reached => reconnecting?
            if($self->reconnect_after_timeout || !($dbh->ping)){
                #ping failed, so trying to reconnect";
                $dbh->disconnect;
                $dbh = $self->_build_dbh();
                $instances->{$self}->{dbh} = $dbh;
            }
            $instances->{$self}->{start_time} = time;

        }

    }

    $dbh;
}

sub _build_dbh {
    my $self = $_[0];
    my $opts = {
        AutoCommit => 1,
        RaiseError => 1,
        mysql_auto_reconnect => 1,
    };
    DBI->connect($self->data_source, $self->username, $self->password, $opts);
}

sub transaction {
    my ($self, $sub) = @_;

    if ($self->{_tx}) {
        return $sub->();
    }

    my $dbh = $self->dbh;
    my @res;

    eval {
        $self->{_tx} = 1;
        $dbh->begin_work;
        @res = $sub->();
        $dbh->commit;
        $self->{_tx} = 0;
        1;
    } or do {
        my $err = $@;
        eval { $dbh->rollback };
        $self->{_tx} = 0;
        die $err;
    };

    @res;
}

sub DEMOLISH {
    my ($self) = @_;
    my $dbh = $self->dbh(1);
    $dbh->disconnect if $dbh;
}

1;

=head1 NAME

Catmandu::Store::DBI - A Catmandu::Store plugin for DBI based interfaces

=head1 VERSION

Version 0.0424

=head1 SYNOPSIS

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

The L<catmandu> command line client can be used like this:

    catmandu import JSON to DBI --data_source SQLite:mydb.sqlite < data.json

=head1 DESCRIPTION

A Catmandu::Store::DBI is a Perl package that can store data into
DBI backed databases. The database as a whole is called a 'store'
(L<Catmandu::Store>. Databases also have compartments (e.g. tables)
called 'bags' (L<Catmandu::Bag>).

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

=head1 AUTHOR

Nicolas Steenlant, C<< <nicolas.steenlant at ugent.be> >>

=head1 CONTRIBUTOR

Vitali Peil C<< <vitali.peil at uni-bielefeld.de> >>

=head1 CONTRIBUTOR

Nicolas Franck C<< <nicolas.franck at ugent.be> >>

=head1 SEE ALSO

L<Catmandu::Bag>, L<Catmandu::Searchable>, L<DBI>

=cut
