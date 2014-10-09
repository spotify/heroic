package com.spotify.heroic.filter;

import java.util.List;

public interface ManyTermsFilter extends Filter {
    public List<Filter> terms();
}
