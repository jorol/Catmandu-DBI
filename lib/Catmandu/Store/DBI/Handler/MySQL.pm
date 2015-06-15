package Catmandu::Store::DBI::Handler::MySQL;

use Catmandu::Sane;
use Moo;
use namespace::clean;

extends 'Catmandu::Store::DBI::Handler';

# text is case-insensitive in MySQL
sub string_type {
    'BLOB';
}

sub binary_type {
    'LONGBLOB';
}

sub add_row {
    my ($self, $bag, $row) = @_;
    my @cols = keys %$row;
    my @values = values %$row;
    my $name = $bag->name;
    my $sql = "INSERT INTO $name(".join(',', @cols).") VALUES(".
        join(',', ('?') x @cols).") ON DUPLICATE KEY UPDATE ".
        join(',', map { "$_=VALUES($_)" } @cols);

    my $dbh = $bag->store->dbh;
    my $sth = $dbh->prepare_cached($sql)
        or Catmandu::Error->throw($dbh->errstr);
    $sth->execute(@values) or Catmandu::Error->throw($sth->errstr);
    $sth->finish;
}

1;

