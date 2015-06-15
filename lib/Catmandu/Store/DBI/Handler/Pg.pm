package Catmandu::Store::DBI::Handler::Pg;

use Catmandu::Sane;
use DBD::Pg ();
use Moo;
use namespace::clean;

extends 'Catmandu::Store::DBI::Handler';

sub binary_type {
    'BYTEA';
}

around column_type => sub {
    my ($super, $self, $map) = @_;
    my $sql = $self->$super($map);
    if ($map->{array}) {
        $sql .= '[]';
    }
    $sql;
};

# see 
# http://stackoverflow.com/questions/15840922/where-not-exists-in-postgresql-gives-syntax-error
# and
# https://rt.cpan.org/Public/Bug/Display.html?id=13180
sub add_row {
    my ($self, $bag, $row) = @_;
    my $mapping = $bag->mapping;
    my $id_col = $mapping->{_id}{column};
    my %binary_cols;
    for my $map (values %$mapping) {
        $binary_cols{$map->{column}} = 1 if $map->{type} eq 'binary';
    }
    my $id = $row->{$id_col};
    my @cols = keys %$row;
    my @values = values %$row;
    my $name = $bag->name;
    my $insert_sql = "INSERT INTO $name(".join(',', @cols).") SELECT ".
        join(',', ('?') x @cols).
        " WHERE NOT EXISTS (SELECT 1 FROM $name WHERE $id_col=?)";
    my $update_sql = "UPDATE $name SET ".join(',', map { "$_=?" } @cols).
        " WHERE $id_col=?";

    my $dbh = $bag->store->dbh;
    my $sth = $dbh->prepare_cached($update_sql)
        or Catmandu::Error->throw($dbh->errstr);
    my $i = 0;
    for (; $i < @cols; $i++) {
        my $col = $cols[$i];
        my $val = $vals[$i];
        if ($binary_cols{$col}) {
            $sth->bind_param($i+1, $val, {pg_type => DBD::Pg->PG_BYTEA});
        } else {
            $sth->bind_param($i+1, $val);
        }
    }
    $sth->bind_param($i+1, $id);
    $sth->execute or Catmandu::Error->throw($sth->errstr);

    unless ($sth->rows) {
        $sth->finish;
        $sth = $dbh->prepare_cached($insert_sql)
            or Catmandu::Error->throw($dbh->errstr);
        my $i = 0;
        for (; $i < @cols; $i++) {
            my $col = $cols[$i];
            my $val = $vals[$i];
            if ($binary_cols{$col}) {
                $sth->bind_param($i+1, $val, {pg_type => DBD::Pg->PG_BYTEA});
            } else {
                $sth->bind_param($i+1, $val);
            }
        }
        $sth->bind_param($i+1, $id);
        $sth->execute or Catmandu::Error->throw($sth->errstr);
    }
    $sth->finish;
}

1;

