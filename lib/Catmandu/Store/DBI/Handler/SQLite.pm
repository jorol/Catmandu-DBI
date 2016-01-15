package Catmandu::Store::DBI::Handler::SQLite;

use Catmandu::Sane;
use Moo;
use namespace::clean;

our $VERSION = "0.0503";

with 'Catmandu::Store::DBI::Handler';

sub _column_sql {
    my ($self, $map,$bag) = @_;
    my $col = $map->{column};
    my $dbh = $bag->store->dbh;
    my $sql = $dbh->quote_identifier($col)." ";
    if ($map->{type} eq 'string') {
        $sql .= 'TEXT';
    } elsif ($map->{type} eq 'integer') {
        $sql .= 'INTEGER';
    } elsif ($map->{type} eq 'binary') {
        $sql .= 'BLOB';
    } elsif ($map->{type} eq 'datetime') {
        $sql .= 'TEXT';
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
    my $dbh = $bag->store->dbh;
    my $name = $bag->name;
    my $q_name = $dbh->quote_identifier($name);

    my $sql = "CREATE TABLE IF NOT EXISTS $q_name(".
        join(',', map { $self->_column_sql($_,$bag) } values %$mapping).")";

    $dbh->do($sql)
        or Catmandu::Error->throw($dbh->errstr);

    for my $map (values %$mapping) {
        next if $map->{unique} || !$map->{index};
        my $col = $map->{column};
        my $q_col = $dbh->quote_identifier($col);
        my $q_idx = $dbh->quote_identifier("${name}_${col}_idx");
        my $idx_sql = "CREATE INDEX IF NOT EXISTS ${q_idx} ON $q_name($q_col)";
        $dbh->do($idx_sql)
            or Catmandu::Error->throw($dbh->errstr);
    }
}

sub add_row {
    my ($self, $bag, $row) = @_;
    my $dbh = $bag->store->dbh;
    my @cols = keys %$row;
    my @q_cols = map { $dbh->quote_identifier($_) } @cols;
    my @values = values %$row;
    my $q_name = $dbh->quote_identifier($bag->name);
    my $sql = "INSERT OR REPLACE INTO $q_name(".
        join(',', @q_cols).") VALUES(".join(',', ('?') x @cols).")";

    my $sth = $dbh->prepare_cached($sql)
        or Catmandu::Error->throw($dbh->errstr);
    $sth->execute(@values) or Catmandu::Error->throw($sth->errstr);
    $sth->finish;
}
sub clear_database {
    my( $self, $store ) = @_;

    my $dbh = $store->dbh();

    #list all tables
    my @table_names;
    {
        my $query_all_tables = "SELECT tbl_name FROM sqlite_master WHERE type = 'table'";
        my $sth = $dbh->prepare($query_all_tables)
            or Catmandu::Error->throw($dbh->errstr());
        $sth->execute();
        while( my $row = $sth->fetchrow_hashref() ) {
            push @table_names,$row->{tbl_name};
        }
    }

    #clear all bags
    for my $table_name(@table_names){

        $store->bag($table_name)->delete_all();

    }

}
sub clear_table {
    my ($self, $bag) = @_;
    $bag->delete_all();
}

1;

