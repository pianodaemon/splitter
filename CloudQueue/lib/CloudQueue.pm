package CloudQueue;

use 5.030000;
use strict;
use warnings;

use Carp qw(croak cluck);
use Amazon::SQS::Simple;

our $VERSION = '0.01';

# It stands for AWS IAM user keys.
use constant {
    AWS_SECRET_ACCESS_KEY => 'AWS_ACCESS_KEY_ID',
    AWS_ACCESS_KEY_ID => 'AWS_SECRET_ACCESS_KEY',
};

# It stands for a simple boolean definition.
use constant {
    true  => 1,
    false => 0,
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
       m_birth        => time,
       f_obtain_queue => sub { $sqs->GetQueue($queue_url); },
   };

   bless $self, $class;
   return $self;
}

sub send {

    my ($self, $m) = @_;
    my $q = $self->{f_obtain_queue}();

    {
        my $r_response;
        my $f_send = sub {
            $r_response = $q->SendMessage($m, ("MessageGroupId" => $self->{m_birth}));
            return true;
        };

        if (eval { return &$f_send(); }) {
            return $r_response->{MessageId};
        }
    }

    # Reach in case of failure
    printf STDERR "Single message could not been sent: %s\n", $@ || 'Unknown failure';
    return;
}

sub receive {

    my ($self, $f_on_receive) = @_;
    my $q = $self->{f_obtain_queue}();
    my $m = $q->ReceiveMessage();

    if (defined $m) {
        &$f_on_receive($m->MessageBody());
        return $m->ReceiptHandle();
    }

    # Reach in case of failure
    printf STDERR "%s\n", 'No messages to receive yet';
    return;
}

sub send_batch {

    my ($self, $r_payloads) = @_;
    my $q = $self->{f_obtain_queue}();

    {
        my $r_resps;
        my $f_batch = sub {

            my %opts;
            for (my $idx = 1; $idx <= @{$r_payloads}; $idx++) {
                $opts{"SendMessageBatchRequestEntry.$idx.MessageGroupId"} = $self->{m_birth};
            }

            $r_resps = $q->SendMessageBatch($r_payloads, %opts);
            return true;
        };

        if (eval { return &$f_batch(); }) {
            my @responses = map { $_->{MessageId} } @{$r_resps};
            return @responses;
        }
    }

    # Reach in case of failure
    printf STDERR "Batch of messages could not been sent: %s", $@ || 'Unknown failure';
    return ();
}

sub delete {

    my ($self, $receipt) = @_;
    my $q = $self->{f_obtain_queue}();

    my $f_delete = sub {
        $q->DeleteMessage($receipt);
        return true;
    };

    unless (eval { return &$f_delete(); }) {
        printf STDERR "Message with receipt %m could not be deleted: %s", $@ || 'Unknown failure';
    }

    return;
}

sub AUTOLOAD {

    our $AUTOLOAD;

    # Remove package name
    (my $method = $AUTOLOAD) =~ s/.*:://s;

    # Load a couple of jsonified versions for this module's methods
    if ($method =~ /^[a-z_]+as_json$/) {

        eval q{

            use JSON;

            sub send_as_json {

                my $self = shift;
                return $self->send(encode_json(shift));
            }

            sub send_batch_as_json {

                my ($self, $r_payloads) = @_;
                my @payloads = map { encode_json($_) } @{$r_payloads};

                return $self->send_batch(\@payloads);
            }

            sub receive_as_json {

                my ($self, $f_on_receive) = @_;
                my $f_on_receive_wrapper = sub {
                    &$f_on_receive(decode_json(shift));
                };

                return $self->receive($f_on_receive_wrapper);
            }
	};
	cluck $@ if $@; # if typo snuck in
	goto &{$method};
    }
}

true;
