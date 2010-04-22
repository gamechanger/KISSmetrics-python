import re


def is_robot(user_agent):
  if user_agent:
    user_agent = unicode(user_agent).lower()

    # We mark something as a bot if it contains any of the $bot_indicators
    # or if it does not contain one of the $browser_indicators. In addition,
    # if the user-agent string contains "mozilla" we make sure it has version
    # information. Finally anything that starts with a word in the $whitelist
    # is never considered a bot.

    whitelist = ('w3m', 'dillo', 'links', 'elinks', 'lynx')
    for agent in whitelist:
        if agent in user_agent:
            return False

    bot_indicators = ('bot', 'spider', 'search', 'jeeves', 'crawl', 'seek',
                      'heritrix', 'slurp', 'thumbnails', 'capture', 'ferret',
                      'webinator', 'scan', 'retriever', 'accelerator',
                      'upload', 'digg', 'extractor', 'grub', 'scrub')
    for agent in bot_indicators:
        if agent in user_agent:
            return True

    browser_indicators = ('mozilla', 'browser', 'iphone', 'lynx', 'mobile',
                          'opera', 'icab')
    has_browser_indicator = False
    for agent in browser_indicators:
        if agent in user_agent:
            has_browser_indicator = True
            break

    if not has_browser_indicator:
        return True

    # Check for mozilla version information
    if 'mozilla' in user_agent:
        if '(' not in user_agent:
            return True
        if not re.search(r'mozilla/\d+', user_agent):
            return True

  return False
