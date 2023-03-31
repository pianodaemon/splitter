package CloudQueue;

use 5.030000;
use strict;
use warnings;
use Amazon::SQS::Simple;

our $VERSION = '0.01';

# It stands for AWS IAM user keys.
use constant {
    AWS_SECRET_ACCESS_KEY => 'AWS_ACCESS_KEY_ID',
    AWS_ACCESS_KEY_ID => 'AWS_SECRET_ACCESS_KEY',
};

# It stands for a simple boolean definition.
use constant {
    True  => 1,
    False => 0,
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

    $q->SendMessage($m, ("MessageGroupId" => 1));
}

sub receive {

    my ($self, $f_on_receive) = @_;
    my $q = $self->{f_obtain_queue}();
    my $m = $q->ReceiveMessage();

    if (defined $m) {
        &$f_on_receive($m->MessageBody());
        return True;
    }

    return False;
}

sub delete {

    my ($self, $m) = @_;
    my $q = $self->{f_obtain_queue}();

    $q->DeleteMessage($m);
}

1;
