package Catmandu::Store::DBI::Handler::SQLite;

use Catmandu::Sane;
use Moo;
use namespace::clean;

with 'Catmandu::Store::DBI::Handler';

sub _column_sql {
    my ($self, $map) = @_;
    my $col = $map->{column};
    my $sql = "$col ";
    if ($map->{type} eq 'string') {
        $sql .= 'TEXT';
    } elsif ($map->{type} eq 'integer') {
        $sql .= 'INTEGER';
    } elsif ($map->{type} eq 'binary') {
        $sql .= 'BLOB';
    }
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
        join(',', map { $self->_column_sql($_) } values %$mapping).")";

    $dbh->do($sql)
        or Catmandu::Error->throw($dbh->errstr);

    for my $map (values %$mapping) {
        next if $map->{unique} || !$map->{index};
        my $col = $map->{column};
        my $idx_sql = "CREATE INDEX IF NOT EXISTS ${name}_${col}_idx ON $name($col)";
        $dbh->do($idx_sql)
            or Catmandu::Error->throw($dbh->errstr);
    }
}

sub add_row {
    my ($self, $bag, $row) = @_;
    my @cols = keys %$row;
    my @values = values %$row;
    my $name = $bag->name;
    my $sql = "INSERT OR REPLACE INTO $name(".
        join(',', @cols).") VALUES(".join(',', ('?') x @cols).")";

    my $dbh = $bag->store->dbh;
    my $sth = $dbh->prepare_cached($sql)
        or Catmandu::Error->throw($dbh->errstr);
    $sth->execute(@values) or Catmandu::Error->throw($sth->errstr);
    $sth->finish;
}

1;

