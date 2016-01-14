package Catmandu::Store::DBI::Bag;

use Catmandu::Sane;
use Moo;
use Catmandu::Store::DBI::Iterator;
use namespace::clean;

our $VERSION = "0.0503";

my $default_mapping = {
    _id => {
        column => 'id',
        type => 'string',
        index => 1,
        required => 1,
        unique => 1,
    },
    _data => {
        column => 'data',
        type => 'binary',
        serialize => 'all',
    }
};

has mapping => (
    is => 'ro',
    default => sub { +{%$default_mapping} },
);

has _iterator => (
    is => 'ro',
    lazy => 1,
    builder => '_build_iterator',
    handles => [qw(
        generator
        count
        slice
        select
        detect
        first
    )],
);

with 'Catmandu::Bag';
with 'Catmandu::Serializer';

sub BUILD {
    my ($self) = @_;
    $self->_normalize_mapping;
    # TODO should happen lazily;
    $self->store->handler->create_table($self);
}

sub _normalize_mapping {
    my ($self) = @_;
    my $mapping = $self->mapping;

    $mapping->{_id} ||= $default_mapping->{_id};

    for my $key (keys %$mapping) {
        my $map = $mapping->{$key};
        $map->{type}   ||= 'string';
        $map->{column} ||= $key;
    }

    $mapping;
}

sub _build_iterator {
    my ($self) = @_;
    Catmandu::Store::DBI::Iterator->new(bag => $self);
}

sub get {
    my ($self, $id) = @_;
    my $store = $self->store;
    my $dbh = $store->dbh;
    my $q_name = $dbh->quote_identifier($self->name);
    my $q_id_field = $dbh->quote_identifier( $self->mapping->{_id}->{column} );
    my $sth = $dbh->prepare_cached("SELECT * FROM ${q_name} WHERE ${q_id_field}=?")
        or Catmandu::Error->throw($dbh->errstr);
    $sth->execute($id) or Catmandu::Error->throw($sth->errstr);
    my $row = $sth->fetchrow_hashref;
    $sth->finish;
    $self->_row_to_data($row // return);
}

sub add {
    my ($self, $data) = @_;
    $self->store->handler->add_row($self, $self->_data_to_row($data));
    $data;
}

sub delete {
    my ($self, $id) = @_;
    my $store = $self->store;
    my $dbh = $store->dbh;
    my $q_name = $dbh->quote_identifier($self->name);
    my $q_id_field = $dbh->quote_identifier( $self->mapping->{_id}->{column} );
    my $sth = $dbh->prepare_cached("DELETE FROM ${q_name} WHERE ${q_id_field}=?")
      or Catmandu::Error->throw($dbh->errstr);
    $sth->execute($id) or Catmandu::Error->throw($sth->errstr);
    $sth->finish;
}

sub delete_all {
    my ($self) = @_;
    my $store = $self->store;
    my $dbh = $store->dbh;
    my $q_name = $dbh->quote_identifier($self->name);
    my $sth = $dbh->prepare_cached("DELETE FROM ${q_name}")
        or Catmandu::Error->throw($dbh->errstr);
    $sth->execute or Catmandu::Error->throw($sth->errstr);
    $sth->finish;
}

sub _row_to_data {
    my ($self, $row) = @_;
    my $mapping = $self->mapping;
    my $data = {};

    for my $key (keys %$mapping) {
        my $map = $mapping->{$key};
        my $val = $row->{$map->{column}} // next;
        if ($map->{serialize}) {
            $val = $self->deserialize($val);
            if ($map->{serialize} eq 'all') {
                for my $k (keys %$val) {
                    $data->{$k} = $val->{$k} // next;
                }
                next;
            }
        }
        if ($map->{type} eq "datetime") {

            my($date,$time) = split ' ', $val;
            $val = "${date}T${time}Z";

        }
        $data->{$key} = $val;
    }

    $data;
}

sub _data_to_row {
    my ($self, $data) = @_;
    $data = {%$data};
    my $mapping = $self->mapping;
    my $row = {};
    my $serialize_all_column;

    for my $key (keys %$mapping) {
        my $map = $mapping->{$key};
        my $val = delete($data->{$key});
        if ($map->{serialize}) {
            if ($map->{serialize} eq 'all') {
                $serialize_all_column = $map->{column};
                next;
            }
            $val = $self->serialize($val // next);
        }
        if ($map->{type} eq "datetime") {

            chop($val);
            $val = join(' ',split('T',$val));

        }
        $row->{$map->{column}} = $val // next;
    }

    if ($serialize_all_column) {
        $row->{$serialize_all_column} = $self->serialize($data);
    }

    $row;
}
sub drop {
    my $self = $_[0];
    $self->store->handler->drop_table($self);
}

1;

