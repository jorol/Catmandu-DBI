package Catmandu::Store::DBI::Iterator;

use Catmandu::Sane;
use Catmandu::Util qw(is_value is_string is_array_ref);
use Moo;
use namespace::clean;

with 'Catmandu::Iterable';

has bag => (is => 'ro', required => 1);
has where => (is => 'ro');
has binds => (is => 'lazy');
has total => (is => 'ro');
has start => (is => 'lazy');
has limit => (is => 'lazy');
has _select_sql => (is => 'ro', lazy => 1, builder => '_build_select_sql');
has _count_sql => (is => 'ro', lazy => 1, builder => '_build_count_sql');

sub _build_binds { [] }
sub _build_start { 0 }
sub _build_limit {
    my ($self) = @_;
    my $limit = 100;
    my $total = $self->total;
    if (defined $total && $total < $limit) {
        $limit = $total;
    }
    $limit;
}

sub _build_select_sql {
    my ($self) = @_;
    my $where = $self->where;
    my $limit = $self->limit;
    my $sql = "SELECT * FROM ".$self->bag->name;
    $sql .= " WHERE $where" if $where;
    $sql .= " ORDER BY id LIMIT $limit OFFSET ?";
    $sql;
}

sub _build_count_sql {
    my ($self) = @_;
    my $bag = $self->bag;
    my $name = $bag->name;
    my $where = $self->where;
    my $sql = "SELECT COUNT(*)";
    $sql .= "FROM $name";
    if ($where) {
        $sql .= " WHERE $where";
    }
    $sql;
}

sub generator {
    my ($self) = @_;
    my $bag = $self->bag;
    my $mapping = $bag->mapping;
    my $sql = $self->_select_sql;
    my $binds = $self->binds;
    my $total = $self->total;
    my $start = $self->start;
    my $limit = $self->limit;

    sub {
        state $rows;

        return if defined $total && !$total;

        unless (defined $rows && @$rows) {
            my $dbh = $bag->store->dbh;
            my $sth = $dbh->prepare_cached($sql)
                or Catmandu::Error->throw($dbh->errstr);
            $sth->execute(@$binds, $start)
                or Catmandu::Error->throw($sth->errstr);
            $rows = $sth->fetchall_arrayref($mapping ? {} : ());
            $sth->finish;
            $start += $limit;
        }

        my $row = shift(@$rows) // return;
        my $data;
        if ($mapping) {
            $data = $bag->deserialize($row->{data});
            for my $field (keys %$mapping) {
                my $val = $row->{$field};
                $data->{$field} = $val if defined $val;
            }
        } else {
            $data = $bag->deserialize($row->[0]);
        }

        $total-- if defined $total;
        $data;
    };
}

sub count {
    my ($self) = @_;
    my $binds = $self->binds;
    my $dbh = $self->bag->store->dbh;
    my $sth = $dbh->prepare_cached($self->_count_sql)
        or Catmandu::Error->throw($dbh->errstr);
    $sth->execute(@$binds)
        or Catmandu::Error->throw($sth->errstr);
    my ($n) = $sth->fetchrow_array;
    $sth->finish;
    $n;
}

sub slice {
    my ($self, $start, $total) = @_;
    ref($self)->new({
        bag => $self->bag,
        where => $self->where,
        binds => $self->binds,
        total => $total,
        start => $self->start + ($start // 0),
    });
}

around select => sub {
    my ($orig, $self, $arg1, $arg2) = @_;
    my $mapping = $self->bag->mapping;

    if ($mapping && 
            is_string($arg1) && 
            $mapping->{$arg1} &&
            (is_value($arg2) || is_array_ref($arg2))) {
        my $opts = $self->_scope($arg1, $arg2);
        return ref($self)->new($opts);
    }

    $self->$orig($arg1, $arg2);
};

around detect => sub {
    my ($orig, $self, $arg1, $arg2) = @_;
    my $mapping = $self->bag->mapping;

    if ($mapping && 
            is_string($arg1) && 
            $mapping->{$arg1} &&
            (is_value($arg2) || is_array_ref($arg2))) {
        my $opts = $self->_scope($arg1, $arg2);
        $opts->{total} = 1;
        return ref($self)->new($opts)->generator->();
    }

    $self->$orig($arg1, $arg2);
};

sub first {
    my ($self) = @_;
    ref($self)->new({
        bag => $self->bag,
        where => $self->where,
        binds => $self->binds,
        total => 1,
        start => $self->start,
    })->generator->();
}

sub _scope {
    my ($self, $arg1, $arg2) = @_;
    my $spec = $self->bag->mapping->{$arg1};
    my $binds = [@{$self->binds}];
    my $where = is_string($self->where) ? '('.$self->where.') AND ': '';

    if ($spec->{array}) {
        push @$binds, is_value($arg2) ? [$arg2] : $arg2;
        $where .= "($arg1 && ?)";
    } elsif (is_value($arg2)) {
        push @$binds, $arg2;
        $where .= "($arg1=?)";
    } else {
        push @$binds, @$arg2;
        $where .= "($arg1 IN(".join(',', ('?') x @$arg2).'))';
    }

    {
        bag => $self->bag,
        where => $where,
        binds => $binds,
        start => $self->start,
    };
}

1;
