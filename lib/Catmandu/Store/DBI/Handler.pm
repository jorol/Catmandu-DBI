package Catmandu::Store::DBI::Handler;

use Catmandu::Sane;
use Moo::Role;
use namespace::clean;

requires 'string_type';
requires 'integer_type';
requires 'binary_type';
requires 'create_index';
requires 'add_row';

sub column_type {
    my ($self, $map) = @_; 
    if ($map->{type} eq 'string') {
        $self->string_type;
    } elsif ($map->{type} eq 'integer') {
        $self->integer_type;
    } elsif ($map->{type} eq 'binary') {
        $self->binary_type;
    }
}

sub column_sql {
    my ($self, $map) = @_; 
    my $sql = "$map->{column} ";
    $sql .= $self->column_type($map);
    if ($map->{unique}) {
        $sql .= " UNIQUE";
    }
    if ($map->{required}) {
        $sql .= " NOT NULL";
    }
    $sql;
}

sub create_table {
    my ($self, $bag) = @_;
    my $mapping = $bag->mapping;
    my $name = $bag->name;
    my $dbh = $bag->store->dbh;
    my $sql = "CREATE TABLE IF NOT EXISTS $name(".
        join(',', map { $self->column_sql($_) } values %$mapping).")";
    $dbh->do($sql) or Catmandu::Error->throw($dbh->errstr);
}

sub create_indexes {
    my ($self, $bag) = @_;
    my $mapping = $bag->mapping;
    for my $map (values %$mapping) {
        next if !$map->{index} || $map->{unique};
        $self->create_index($bag, $map);
    }
}

1;

