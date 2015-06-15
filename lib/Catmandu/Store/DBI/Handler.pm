package Catmandu::Store::DBI::Handler;

use Catmandu::Sane;
use Moo;
use namespace::clean;

sub string_type {
    'TEXT';
}

sub integer_type {
    'INTEGER';
}

sub binary_type {
    'BLOB';
}

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
    my $name = $bag->name;
    my $dbh = $bag->store->dbh;
    die "TODO";
}

sub add_row {
    my ($self, $bag, $row) = @_;
    my $id_col = $bag->mapping->{_id}{column};
    my $id = $row->{$id_col};
    my @cols = keys %$row;
    my @values = values %$row;
    my $name = $bag->name;
    my $insert_sql = "INSERT INTO $name(".join(',', @cols).") VALUES(".
        join(',', ('?') x @cols).")".
        " WHERE NOT EXISTS (SELECT 1 FROM $name WHERE $id_col=?)";
    my $update_sql = "UPDATE $name SET ".join(',', map { "$_=?" } @cols).
        " WHERE $id_col=?";

    my $dbh = $bag->store->dbh;
    my $sth = $dbh->prepare_cached($update_sql)
        or Catmandu::Error->throw($dbh->errstr);
    $sth->execute(@values, $id) or Catmandu::Error->throw($sth->errstr);
    unless ($sth->rows) {
        $sth->finish;
        $sth = $dbh->prepare_cached($insert_sql)
            or Catmandu::Error->throw($dbh->errstr);
        $sth->execute(@values, $id)
            or Catmandu::Error->throw($sth->errstr);
    }
    $sth->finish;
}

1;

