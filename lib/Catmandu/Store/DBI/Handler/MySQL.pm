package Catmandu::Store::DBI::Handler::MySQL;

use Catmandu::Sane;
use Moo;
use namespace::clean;

with 'Catmandu::Store::DBI::Handler';

# text types are case-insensitive in MySQL
sub _column_sql {
    my ($self, $map) = @_;
    my $col = $map->{column};
    my $sql = "$col ";
    if ($map->{type} eq 'string' && $map->{unique}) {
        $sql .= 'VARCHAR(255) BINARY';
    } elsif ($map->{type} eq 'string') {
        $sql .= 'TEXT BINARY';
    } elsif ($map->{type} eq 'integer') {
        $sql .= 'INTEGER';
    } elsif ($map->{type} eq 'binary') {
        $sql .= 'LONGBLOB';
    }
    if ($map->{unique}) {
        $sql .= " UNIQUE";
    }
    if ($map->{required}) {
        $sql .= " NOT NULL";
    }
    if (!$map->{unique} && $map->{index}) {
        if ($map->{type} eq 'string') {
            $sql .= ", INDEX($col(255))";
        } else {
            $sql .= ", INDEX($col)";
        }
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
}

sub add_row {
    my ($self, $bag, $row) = @_;
    my @cols = keys %$row;
    my @vals = values %$row;
    my $name = $bag->name;
    my $sql = "INSERT INTO $name(".join(',', @cols).") VALUES(".
        join(',', ('?') x @cols).") ON DUPLICATE KEY UPDATE ".
        join(',', map { "$_=VALUES($_)" } @cols);

    my $dbh = $bag->store->dbh;
    my $sth = $dbh->prepare_cached($sql)
        or Catmandu::Error->throw($dbh->errstr);
    $sth->execute(@vals) or Catmandu::Error->throw($sth->errstr);
    $sth->finish;
}

1;

