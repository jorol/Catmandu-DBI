package Catmandu::Store::DBI::Handler::SQLite;

use Catmandu::Sane;
use Moo;
use namespace::clean;

with 'Catmandu::Store::DBI::Handler';

sub string_type {
    'TEXT';
}

sub integer_type {
    'INTEGER';
}

sub binary_type {
    'BLOB';
}

sub create_index {
    my ($self, $bag, $map) = @_;
    my $name = $bag->name;
    my $col = $map->{column};
    my $dbh = $bag->store->dbh;
    my $sql = "CREATE INDEX IF NOT EXISTS ${name}_${col}_idx ON $name($col)";
    $dbh->do($sql) or Catmandu::Error->throw($dbh->errstr);
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

