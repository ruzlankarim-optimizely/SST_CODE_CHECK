CASE
  WHEN OPPORTUNITY.Opportunity_Source_c IN (
    'AE/AM Generated',
    'Welcome',
    'Optimizely',
    'Empire Selling - AE',
    'Generated - AE/AM',
    'Acquisition'
  ) THEN 'AE'
  WHEN OPPORTUNITY.Opportunity_Source_c IN (
    'Generated - CSM',
    'Generated - Support',
    'Expert Services',
    'Education Services',
    'Education Store',
    'Auto-Renewal',
    'CSM Generated',
    'Support Generated',
    'Expert Services',
    'Education Services',
    'Education Store'
  ) THEN 'CSM'
  WHEN OPPORTUNITY.Opportunity_Source_c IN (
    'LeadGenius',
    'Event - Sponsored',
    'Event - Hosted',
    'Webinar - Sponsored',
    'Webinar - Hosted',
    'UserGems',
    'G2',
    'Content Syndication',
    'Search - Organic',
    'Social - Organic',
    'Search - Paid',
    'Social - Paid',
    'Website - Direct',
    'Website - Referral',
    'Display - Paid',
    'Sponsorship - Paid',
    'Purchased List',
    'Email',
    'Direct Email',
    'Peer Review Website',
    'Google Marketplace',
    'Partner Marketing',
    'Content download web',
    'Website Direct',
    'Marketing',
    'Live Event',
    'Content Syndication',
    'UserGems',
    'Demo request web',
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
  ) THEN 'Marketing'
  WHEN OPPORTUNITY.Opportunity_Source_c IN (
    'Generated - Technology Partner',
    'Generated - Partner',
    'Referral',
    'Partner',
    'Partner Marketing',
    'Referral',
    'Technology Partner'
  ) THEN 'Partner'
  WHEN OPPORTUNITY.Opportunity_Source_c IN (
    'Generated - SDR',
    'Zoominfo',
    'Lead IQ',
    'Lusha',
    'LinkedIn Sales Navigator',
    'SDR Generated',
    'Lead IQ',
    'DiscoverOrg',
    'Empire Selling - SDR',
    'Zoominfo'
  ) THEN 'SDR'
  ELSE 'Unknown'
END AS Opportunity_Source_Category__c