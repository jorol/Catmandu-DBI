package Catmandu::Importer::DBI;

use Catmandu::Sane;
use DBI;
use Moo;

our $VERSION = '0.031';

with 'Catmandu::Importer';

has dsn      => (is => 'ro' , required => 1);
has user     => (is => 'ro');
has password => (is => 'ro');
has query    => (is => 'ro' , required => 1);
has dbh  => (
    is       => 'ro',
    init_arg => undef,
    lazy     => 1,
    builder  => '_build_dbh',
);
has sth  => (
	is       => 'ro',
    init_arg => undef,
    lazy     => 1,
    builder  => '_build_sth',
);

sub _build_dbh {
    my $self = $_[0];
    DBI->connect($self->dsn, $self->user, $self->password);
}

sub _build_sth {
    my $self = $_[0];
    my $sth  = $self->dbh->prepare($self->query);
    $sth->execute;
    $sth;
}

sub generator {
	my ($self) = @_;

	return sub {
		$self->sth->fetchrow_hashref();
	}
}

sub DESTROY {
	my ($self) = @_;
	$self->sth->finish;
	$self->dbh->disconnect;
}

=head1 NAME

Catmandu::Importer::DBI - Catmandu module to import data from any DBI source

=head1 SYNOPSIS

 use Catmandu::Importer::DBI;

 my %attrs = (
        dsn => 'dbi:mysql:foobar' ,
        user => 'foo' ,
        password => 'bar' ,
        query => 'select * from table'
 );

 my $importer = Catmandu::Importer::DBI->new(%attrs);

 # Optional set extra parameters on the database handle
 # $importer->dbh->{LongReadLen} = 1024 * 64;

 $importer->each(sub {
	my $row_hash = shift;
	...
 });


 # or

 $ catmandu convert DBI --dsn dbi:mysql:foobar --user foo --password bar --query "select * from table"

=head1 AUTHORS

 Patrick Hochstenbach, C<< <patrick.hochstenbach at ugent.be> >>

=head1 SEE ALSO

L<Catmandu>, L<Catmandu::Importer> , L<Catmandu::Store::DBI>

=cut

1;
