JMeter Metabase scripts were created to test load for concurrent user logins as well as concurrent user access to reports. Number of concurrent users is configurable by the Loop Count in the Thread Group setting. Number of concurrent users can also be changed with Number of Threads in the Thread Group setting. We created 33 users listed in metabaseusers.txt file.

Java and JMeter are required installations.

Scripts: MetabaseLogin.jmx, MetabaseLoginAndReportSelection.jmx

Scripts are setup to read usernames and passwords from file (metabaseusers.txt).

Scripts have Assertions/Verifications to check page loaded. These are mostly response code checks.

Scripts access Metabase via http://metabase.datateam-cdc-nbs.eqsandbox.com/

Scripts were derived from recordings using BlazeMeter Chrome extension. They were then exported to JMeter jmx filetype and optimized.

User emails were generated using Mailinator.

SR10 is the only report that was scripted.

Recommended improvement: The password should be parameterized since it's the same for all users. This way there wouldn't be a need to pass it in the user file.

Other reports can be scripted using this model.

More information available here: https://cdc-nbs.atlassian.net/wiki/spaces/NM/pages/226164758/Load+test+of+Metabase+with+H2+on+EKS