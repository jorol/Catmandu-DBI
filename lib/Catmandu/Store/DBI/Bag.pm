package Catmandu::Store::DBI::Bag;

use Catmandu::Sane;
use Moo;
use Catmandu::Store::DBI::Iterator;
use namespace::clean;

has mapping => (is => 'ro', default => sub { +{} });
has _iterator => (
    is => 'ro',
    lazy => 1,
    builder => '_build_iterator',
    handles => [qw(
        generator
        count
        slice
        select
        detect
        first      
    )]
);

with 'Catmandu::Bag';
with 'Catmandu::Serializer';

sub BUILD {
    my ($self) = @_;
    $self->_normalize_mapping;
    $self->store->handler->create_table($self);
    $self->store->handler->create_indexes($self);
}

sub _normalize_mapping {
    my ($self) = @_;
    my $mapping = $self->mapping;

    $mapping->{_id} ||= {
        column => 'id',
        type => 'string',
        index => 1,
        required => 1,
        unique => 1,
    };

    $mapping->{_data} ||= {
        column => 'data',
        type => 'binary',
        serialize => 'all',
    };

    for my $key (keys %$mapping) {
        my $map = $mapping->{$key};
        $map->{type}   ||= 'string';
        $map->{column} ||= $key;
    }

    $mapping;
}

sub _build_iterator {
    my ($self) = @_;
    Catmandu::Store::DBI::Iterator->new(bag => $self);
}

#sub _build_add_sqlite {
    #my $self = $_[0];
    #my $name = $self->name;
    #my $sql  = "INSERT OR REPLACE INTO $name(id,data) VALUES(?,?)";
    #sub {
        #my $dbh = $self->store->dbh;
        #my $sth = $dbh->prepare_cached($sql)
          #or Catmandu::Error->throw($dbh->errstr);
        #$sth->execute($_[0], $_[1]) or Catmandu::Error->throw($sth->errstr);
        #$sth->finish;
    #};
#}

#sub _build_create {
    #my $self = $_[0];
    #my $driver_name = $self->store->dbh->{Driver}{Name} // "";
    #if ($driver_name =~ /pg/i) { return $self->_build_create_postgres }
    #elsif ($driver_name =~ /mysql/i) { return $self->_build_create_mysql }
    #$self->_build_create_generic;
#}

sub _build_create_postgres {
    my $self = $_[0];
    my $name = $self->name;
    my $dbh  = $self->store->dbh;
    # requires al least Postgres 9.1
    # TODO get rid of this annoying warning:
    # 'NOTICE:  relation "$name" already exists, skipping'
    local $SIG{__WARN__} = sub { print STDERR $_[0]; };
    my $field_sql = "id VARCHAR(255) PRIMARY KEY, data BYTEA NOT NULL";
    if (my $mapping = $self->mapping) {
        for my $field (sort keys %$mapping) {
            if ($field eq 'id' || $field eq 'data') {
                die "invalid field name";
            }

            my $spec = $mapping->{$field};
            $spec->{type} ||= 'string';

            $field_sql .= ", $field ";

            if ($spec->{type} eq 'string') {
                $field_sql .= "TEXT";
            } elsif ($spec->{type} eq 'integer') {
                $field_sql .= "INTEGER";
            } else {
                die "invalid field type";
            }

            if ($spec->{array}) {
                $field_sql .= "[]";
            }

            if ($spec->{required}) {
                $field_sql .= " NOT NULL";
            }
        }
    }
    my $sql = "CREATE TABLE IF NOT EXISTS $name($field_sql);";
    # TODO alter table if a mapping isn't found
    if (my $mapping = $self->mapping) {
        for my $field (sort keys %$mapping) {
            my $spec = $mapping->{$field};
            $sql .= $self->_postgres_create_index_sql($field) if $spec->{index};
        }
    }
    $dbh->do($sql) or Catmandu::Error->throw($dbh->errstr);
}

sub _postgres_create_index_sql {
    my ($self, $field) = @_;
    my $name = $self->name;
<<SQL
DO \$\$
BEGIN

IF NOT EXISTS (
    SELECT 1
    FROM   pg_class c
    JOIN   pg_namespace n ON n.oid = c.relnamespace
    WHERE  c.relname = '${name}_${field}_idx'
    AND    n.nspname = 'public'
    ) THEN

    CREATE INDEX ${name}_${field}_idx ON public.${name} (${field});
END IF;

END\$\$;
SQL
}

#varchar in mysql is case insensitive
#cf. http://stackoverflow.com/questions/3396253/altering-mysql-table-column-to-be-case-sensitive
#sub _build_create_mysql {
    #my $self = $_[0];
    #my $name = $self->name;
    #my $dbh  = $self->store->dbh;
    #my $sql = "CREATE TABLE IF NOT EXISTS $name(id varchar(255) binary not null primary key, data longblob not null)";
    #$dbh->do($sql) or Catmandu::Error->throw($dbh->errstr);
#}

#sub _build_create_generic {
    #my $self = $_[0];
    #my $name = $self->name;
    #my $dbh  = $self->store->dbh;
    #my $sql = "CREATE TABLE IF NOT EXISTS $name(id varchar(255) not null primary key, data longblob not null)";
    #$dbh->do($sql) or Catmandu::Error->throw($dbh->errstr);
#}

sub get {
    my ($self, $id) = @_;
    my $name = $self->name;
    my $dbh = $self->store->dbh;
    my $sth = $dbh->prepare_cached("SELECT * FROM $name WHERE id=?")
        or Catmandu::Error->throw($dbh->errstr);
    $sth->execute($id) or Catmandu::Error->throw($sth->errstr);
    my $row = $sth->fetchrow_hashref;
    $sth->finish;
    $self->_row_to_data($row // return);
}

sub add {
    my ($self, $data) = @_;
    $self->store->handler->add_row($self, $self->_data_to_row($data));
}

sub delete {
    my ($self, $id) = @_;
    my $name = $self->name;
    my $dbh = $self->store->dbh;
    my $sth = $dbh->prepare_cached("DELETE FROM $name WHERE id=?")
      or Catmandu::Error->throw($dbh->errstr);
    $sth->execute($id) or Catmandu::Error->throw($sth->errstr);
    $sth->finish;
}

sub delete_all {
    my ($self) = @_;
    my $name = $self->name;
    my $dbh = $self->store->dbh;
    my $sth = $dbh->prepare_cached("DELETE FROM $name")
        or Catmandu::Error->throw($dbh->errstr);
    $sth->execute or Catmandu::Error->throw($sth->errstr);
    $sth->finish;
}

sub _row_to_data {
    my ($self, $row) = @_;
    my $mapping = $self->mapping;
    my $data = {};

    for my $key (keys %$mapping) {
        my $map = $mapping->{$key};
        my $val = $row->{$map->{column}} // next;
        if ($map->{serialize}) {
            $val = $self->deserialize($val);
            if ($map->{serialize} eq 'all') {
                for my $k (keys %$val) {
                    $data->{$k} = $val->{$k} // next;
                }
                next;
            }
        }
        $data->{$key} = $val;
    }

    $data;
}

sub _data_to_row {
    my ($self, $data) = @_;
    $data = {%$data};
    my $mapping = $self->mapping;
    my $row = {};
    my $serialize_all_column;

    for my $key (keys %$mapping) {
        my $map = $mapping->{$key};
        my $val = delete($data->{$key}) // next;
        if ($map->{serialize}) {
            if ($map->{serialize} eq 'all') {
                $serialize_all_column = $map->{column};
                next;
            }
            $val = $self->serialize($val);
        }
        $row->{$map->{column}} = $val;
    }

    if ($serialize_all_column) {
        $row->{$serialize_all_column} = $self->serialize($data);
    }

    $row;
}

1;

