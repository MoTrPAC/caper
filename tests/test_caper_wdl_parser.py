from textwrap import dedent

from caper.caper_wdl_parser import CaperWDLParser

WDL_CONTENTS = dedent(
    """\
    version 1.0

    workflow test_wdl {
        meta {
            caper_docker: "ubuntu:latest"
            caper_singularity: "docker://ubuntu:latest"
        }
    }
"""
)


OLD_WDL_CONTENTS = dedent(
    """\
    #CAPER docker "ubuntu:latest"
    #CAPER singularity "docker://ubuntu:latest"

    workflow test_wdl {
    }
"""
)


def test_properties(tmp_path) -> None:
    """Test the following properties.
    - default_docker
    - default_singularity.
    """
    main_wdl = tmp_path / 'main.wdl'
    main_wdl.write_text(WDL_CONTENTS)

    old_wdl = tmp_path / 'old_main.wdl'
    old_wdl.write_text(OLD_WDL_CONTENTS)

    # test reading from workflow.meta
    main = CaperWDLParser(str(main_wdl))
    assert main.default_docker == 'ubuntu:latest'
    assert main.default_singularity == 'docker://ubuntu:latest'

    # test reading from comments (old-style)
    old = CaperWDLParser(str(old_wdl))
    assert old.default_docker == 'ubuntu:latest'
    assert old.default_singularity == 'docker://ubuntu:latest'
