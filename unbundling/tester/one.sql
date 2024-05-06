CASE
  when OPPORTUNITY.Opportunity_Source_c in (
    'AE/AM Generated',
    'Welcome',
    'Optimizely',
    'Empire Selling - AE'
  ) then 'AE'
  when OPPORTUNITY.Opportunity_Source_c in (
    'CSM Generated',
    'Support Generated',
    'Expert Services',
    'Education Services',
    'Education Store'
  ) then 'CSM'
  when OPPORTUNITY.Opportunity_Source_c in (
    'Content download (web)',
    'Website Direct',
    'Marketing',
    'Live Event',
    'Content Syndication',
    'UserGems',
    'Demo request (web)',
    'Drift',
    'Webinar',
    'Tradeshow',
    'Inbound',
    'Paid Search',
    'Content Diagnostic Trial',
    'Purchased List',
    'Organic Search',
    'Event Partner',
    'Virtual Event',
    'Online Advertising',
    'Website Referral',
    'Organic Social',
    'Paid Social'
  ) then 'Marketing'
  when OPPORTUNITY.Opportunity_Source_c in (
    'Partner',
    'Partner Marketing',
    'Referral',
    'Technology Partner'
  ) then 'Partner'
  when OPPORTUNITY.Opportunity_Source_c in (
    'SDR Generated',
    'Lead IQ',
    'DiscoverOrg',
    'Empire Selling - SDR',
    'Zoominfo'
  ) then 'SDR'
  else 'Unknown'
end as Opportunity_Source_Category__c