package Catmandu::Store::DBI::Handler::MySQL;

use Catmandu::Sane;
use Moo;
use namespace::clean;

with 'Catmandu::Store::DBI::Handler';

# text is case-insensitive in MySQL
sub string_type {
    'BLOB';
}

sub integer_type {
    'INTEGER';
}

sub binary_type {
    'LONGBLOB';
}

sub create_index {
    my ($self, $bag, $map) = @_;
    my $name = $bag->name;
    my $col = $map->{column};
    my $dbh = $bag->store->dbh;
    my $idx = "${name}_${col}_idx";
    my $sql = <<SQL;
SELECT IF (
    EXISTS (
        SELECT DISTINCT index_name FROM information_schema.statistics 
        WHERE table_schema = 'schema_db_name' 
        AND table_name = '$name' AND index_name LIKE '$idx'
    )
    ,'SELECT ''INDEX $idx EXISTS'' _______;'
    ,'CREATE INDEX $idx ON $name($col)') INTO \@a;
PREPARE stmt1 FROM \@a;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;
SQL

    $dbh->do($sql) or Catmandu::Error->throw($dbh->errstr);
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

