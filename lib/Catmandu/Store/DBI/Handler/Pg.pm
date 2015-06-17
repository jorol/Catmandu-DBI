package Catmandu::Store::DBI::Handler::Pg;

use Catmandu::Sane;
use DBD::Pg ();
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

around create_table => sub {
    my ($super, $self, $bag) = @_;
    my $name = $bag->name;
    # get rid of annoying warning
    local $SIG{__WARN__} = sub {
        my $msg = $_[0];
        if ($msg !~ /^NOTICE:  relation "$name" already exists/) {
            warn $msg;
        }
    };
    $self->$super($bag);
};

sub create_index {
    my ($self, $bag, $map) = @_;
    my $name = $bag->name;
    my $col = $map->{column};
    my $dbh = $bag->store->dbh;
    my $sql = <<SQL;
DO \$\$
BEGIN

IF NOT EXISTS (
    SELECT 1
    FROM   pg_class c
    JOIN   pg_namespace n ON n.oid = c.relnamespace
    WHERE  c.relname = '${name}_${col}_idx'
    AND    n.nspname = 'public'
    ) THEN

    CREATE INDEX ${name}_${col}_idx ON public.${name} (${col});
END IF;

END\$\$;
SQL

    $dbh->do($sql) or Catmandu::Error->throw($dbh->errstr);
}

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
    my @vals = values %$row;
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

