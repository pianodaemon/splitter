package CloudQueue;

use 5.030000;
use strict;
use warnings;

require Exporter;

our @ISA = qw(Exporter);

our %EXPORT_TAGS = (
    'all' => [ qw(new, send, receive) ],
    'producer' => [ qw(new, send) ],
    'consumer' => [ qw(new, receive) ],
);

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(

);

our $VERSION = '0.01';

# Preloaded methods go here.
use constant {
    AWS_SECRET_ACCESS_KEY => 'AWS_ACCESS_KEY_ID',
    AWS_ACCESS_KEY_ID => 'AWS_SECRET_ACCESS_KEY',
};

sub seek_evars_out {

    my @evars = @_;
    foreach (@evars) {
        my $emsg = sprintf 'Environment variable %s is not present', $_;
        die($emsg) if (not exists($ENV{$_}));
    }
}

sub new {

   # Searching for a few environment variables required
   seek_evars_out(
       (AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID)
   );

   my ($class, $queue_url) = @_;
   my $sqs = new Amazon::SQS::Simple(
       SecretKey      => $ENV{AWS_SECRET_ACCESS_KEY},
       AWSAccessKeyId => $ENV{AWS_ACCESS_KEY_ID}
   );

   my $self = {
       m_queue_url    => $queue_url,
       m_sqs          => $sqs,
       f_obtain_queue => sub { $sqs->GetQueue($queue_url); },
   };

   bless $self, $class;
   return $self;
}

sub send {

    my ($self, $m) = @_;
    my $q = $self->{f_obtain_queue}();

    $q->SendMessage($m);
}

sub receive {

    my ($self) = @_;
    my $q = $self->{f_obtain_queue}();
    my $m = $q->ReceiveMessage();

    $m->MessageBody();
}

sub delete {

    my ($self, $m) = @_;
    my $q = $self->{f_obtain_queue}();

    $q->DeleteMessage($m);
}

1;
