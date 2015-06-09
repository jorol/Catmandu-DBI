package Catmandu::Store::DBI::Bag;

use Catmandu::Sane;
use Moo;
use Catmandu::Store::DBI::Iterator;
use Catmandu::Util qw(require_package);
use namespace::clean;

has mapping => (is => 'ro');

has _sql_get => (is => 'ro', lazy => 1, builder => '_build_sql_get');
has _add => (is => 'ro', lazy => 1, builder => '_build_add');
has _sql_delete => (is => 'ro', lazy => 1, builder => '_build_sql_delete');
has _sql_delete_all =>
  (is => 'ro', lazy => 1, builder => '_build_sql_delete_all');

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
    $_[0]->_build_create;
}

sub _build_iterator {
    my ($self) = @_;
    Catmandu::Store::DBI::Iterator->new(bag => $self);
}

sub _build_sql_get {
    my ($self) = @_;
    my $name = $self->name;
    if (my $mapping = $self->mapping) {
        my $fields = join(',', sort keys %$mapping);
        "SELECT data, $fields FROM $name WHERE id=?";
    } else {
        "SELECT data FROM $name WHERE id=?";
    }
}

sub _build_sql_delete {
    my $name = $_[0]->name;
    "DELETE FROM $name WHERE id=?";
}

sub _build_sql_delete_all {
    my $name = $_[0]->name;
    "DELETE FROM $name";
}

sub _build_add_sqlite {
    my $self = $_[0];
    my $name = $self->name;
    my $sql  = "INSERT OR REPLACE INTO $name(id,data) VALUES(?,?)";
    sub {
        my $dbh = $self->store->dbh;
        my $sth = $dbh->prepare_cached($sql)
          or Catmandu::Error->throw($dbh->errstr);
        $sth->execute($_[0], $_[1]) or Catmandu::Error->throw($sth->errstr);
        $sth->finish;
    };
}

sub _build_add_mysql {
    my $self = $_[0];
    my $name = $self->name;
    my $sql = "INSERT INTO $name(id,data) VALUES(?,?) ON DUPLICATE KEY UPDATE data=VALUES(data)";
    sub {
        my $dbh = $self->store->dbh;
        my $sth = $dbh->prepare_cached($sql)
            or Catmandu::Error->throw($dbh->errstr);
        $sth->execute($_[0], $_[1])
            or Catmandu::Error->throw($sth->errstr);
        $sth->finish;
    };
}

sub _build_add_postgres {
    my ($self)     = @_;
    my $pg         = require_package('DBD::Pg');
    my $name       = $self->name;
    my $mapping    = $self->mapping;

    my $field_count = 2;
    $field_count += scalar(keys $mapping) if $mapping;
    my $insert_field_placeholders = join(',', ('?') x $field_count);
    my $update_field_placeholders = join('', map { ", $_=?" } sort keys %$mapping);

    my $sql_update = "UPDATE $name SET data=?$update_field_placeholders WHERE id=?";
    # see http://stackoverflow.com/questions/15840922/where-not-exists-in-postgresql-gives-syntax-error
    my $sql_insert = "INSERT INTO $name SELECT $insert_field_placeholders WHERE NOT EXISTS (SELECT 1 FROM $name WHERE id=?)";
        
    sub {
        my ($id, $data, $fields) = @_;
        my $dbh = $self->store->dbh;
        my $sth = $dbh->prepare_cached($sql_update)
            or Catmandu::Error->throw($dbh->errstr);

        # special quoting for bytea in postgres:
        # https://rt.cpan.org/Public/Bug/Display.html?id=13180
        # http://www.nntp.perl.org/group/perl.dbi.users/2005/01/msg25370.html
        my $i = 1;
        $sth->bind_param($i++,$data, {pg_type => $pg->PG_BYTEA});
        if ($mapping) {
            for my $field (sort keys %$mapping) {
                $sth->bind_param($i++,$fields->{$field});
            }
        }
        $sth->bind_param($i,$id);

        $sth->execute or Catmandu::Error->throw($sth->errstr);

        unless ($sth->rows) {
            $sth->finish;
            $sth = $dbh->prepare_cached($sql_insert)
              or Catmandu::Error->throw($dbh->errstr);

            my $i = 1;
            $sth->bind_param($i++,$id);
            $sth->bind_param($i++,$data,{pg_type => $pg->PG_BYTEA});
            if ($mapping) {
                for my $field (sort keys %$mapping) {
                    $sth->bind_param($i++,$fields->{$field});
                }
            }
            $sth->bind_param($i,$id);

            $sth->execute or Catmandu::Error->throw($sth->errstr);
            $sth->finish;
        }
    };
}

sub _build_add_generic {
    my $self       = $_[0];
    my $name       = $self->name;
    my $sql_update = "UPDATE $name SET data=? WHERE id=?";
    my $sql_insert = "INSERT INTO $name VALUES(?,?) WHERE NOT EXISTS (SELECT 1 FROM $name WHERE id=?)";
    sub {
        my $dbh = $self->store->dbh;
        my $sth = $dbh->prepare_cached($sql_update)
          or Catmandu::Error->throw( $dbh->errstr );
        $sth->execute($_[1], $_[0]) or Catmandu::Error->throw($sth->errstr);
        unless ($sth->rows) {
            $sth->finish;
            $sth = $dbh->prepare_cached($sql_insert)
              or Catmandu::Error->throw($dbh->errstr);
            $sth->execute( $_[0], $_[1], $_[0] )
              or Catmandu::Error->throw($sth->errstr);
            $sth->finish;
        }
    };
}

sub _build_create {
    my $self = $_[0];
    my $driver_name = $self->store->dbh->{Driver}{Name} // "";
    if ($driver_name =~ /pg/i) { return $self->_build_create_postgres }
    elsif ($driver_name =~ /mysql/i) { return $self->_build_create_mysql }
    $self->_build_create_generic;
}

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
sub _build_create_mysql {
    my $self = $_[0];
    my $name = $self->name;
    my $dbh  = $self->store->dbh;
    my $sql = "CREATE TABLE IF NOT EXISTS $name(id varchar(255) binary not null primary key, data longblob not null)";
    $dbh->do($sql) or Catmandu::Error->throw($dbh->errstr);
}

sub _build_create_generic {
    my $self = $_[0];
    my $name = $self->name;
    my $dbh  = $self->store->dbh;
    my $sql = "CREATE TABLE IF NOT EXISTS $name(id varchar(255) not null primary key, data longblob not null)";
    $dbh->do($sql) or Catmandu::Error->throw($dbh->errstr);
}

sub _build_add {
    my $self = $_[0];
    my $driver_name = $self->store->dbh->{Driver}{Name} // "";
    if ($driver_name =~ /sqlite/i) { return $self->_build_add_sqlite; }
    if ($driver_name =~ /mysql/i)  { return $self->_build_add_mysql; }
    if ($driver_name =~ /pg/i)     { return $self->_build_add_postgres; }
    return $self->_build_add_generic;
}

sub get {
    my ($self, $id) = @_;
    my $dbh = $self->store->dbh;
    my $sth = $dbh->prepare_cached($self->_sql_get)
        or Catmandu::Error->throw($dbh->errstr);
    $sth->execute($id) or Catmandu::Error->throw($sth->errstr);
    my $data;
    my $row = $sth->fetchrow_hashref;
    $sth->finish;
    if ($row) {
        $data = $self->deserialize($row->{data});
        if (my $mapping = $self->mapping) {
            for my $field (keys %$mapping) {
                $data->{$field} = $row->{$field};
            }
        }
    }
    $data;
}

sub add {
    my ($self, $data) = @_;
    if (my $mapping = $self->mapping) {
        my $fields = {};
        for my $field (keys %$mapping) {
            $fields->{$field} = delete $data->{$field};
        }
        $self->_add->($data->{_id}, $self->serialize($data), $fields);
    } else {
        $self->_add->($data->{_id}, $self->serialize($data));
    }
}

sub delete {
    my ($self, $id) = @_;
    my $dbh = $self->store->dbh;
    my $sth = $dbh->prepare_cached($self->_sql_delete)
      or Catmandu::Error->throw($dbh->errstr);
    $sth->execute($id) or Catmandu::Error->throw($sth->errstr);
    $sth->finish;
}

sub delete_all {
    my ($self) = @_;
    my $dbh = $self->store->dbh;
    my $sth = $dbh->prepare_cached($self->_sql_delete_all)
        or Catmandu::Error->throw($dbh->errstr);
    $sth->execute or Catmandu::Error->throw($sth->errstr);
    $sth->finish;
}

1;

